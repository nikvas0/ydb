#include "kqp_sink_common.h"

#include <ydb/core/kqp/rm_service/kqp_snapshot_manager.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/base/tablet_pipecache.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpReadCommitted) {
    class TReadSeesLastCommitted : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto& runtime = *Kikimr->GetTestServer().GetRuntime();

            auto client = Kikimr->GetQueryClient();
            auto session1 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });
            auto session2 = Kikimr->RunCall([&] { return client.GetSession().GetValueSync().GetSession(); });

            size_t evWriteCounter = 0;
            size_t evReadCounter = 0;
            size_t evCreateSnapshotCounter = 0;

            auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                if (ev->GetTypeRewrite() == NKikimr::NEvents::TDataEvents::TEvWrite::EventType) {
                    ++evWriteCounter;
                } else if (ev->GetTypeRewrite() == NKikimr::TEvDataShard::TEvRead::EventType) {
                    ++evReadCounter;
                    auto* evRead = ev->Get<NKikimr::TEvDataShard::TEvRead>();
                    auto& snapshot = evRead->Record.GetSnapshot();
                    UNIT_ASSERT(snapshot.GetStep() != 0);
                    UNIT_ASSERT(snapshot.GetTxId() != 0);
                } else if (ev->GetTypeRewrite() == NKikimr::NKqp::TEvKqpSnapshot::TEvCreateSnapshotRequest::EventType) {
                    ++evCreateSnapshotCounter;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };

            auto saveObserver = runtime.SetObserverFunc(grab);
            Y_DEFER {
                runtime.SetObserverFunc(saveObserver);
            };

            // Session1 starts a Read Committed transaction and reads initial data
            {
                //TEST: TEvCreateSnapshotRequest, EvRead
                auto future1 = Kikimr->RunInThreadPool([&] {
                    return session1.ExecuteQuery(Q_(R"(
                        SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
                    )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
                });

                {
                    TDispatchOptions opts;
                    opts.FinalEvents.emplace_back([&](IEventHandle&) {
                        return evCreateSnapshotCounter >= 1 && evReadCounter >= 1;
                    });
                    runtime.DispatchEvents(opts);
                    UNIT_ASSERT(evCreateSnapshotCounter >= 1);
                    UNIT_ASSERT(evReadCounter >= 1);
                }

                auto result = runtime.WaitFuture(future1);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[[300u];["None"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
                auto tx1 = result.GetTransaction();
                UNIT_ASSERT(tx1);

                // Session2 commits changes to the same data
                {
                    //TEST: EvWrite
                    auto future2 = Kikimr->RunInThreadPool([&] {
                        return session2.ExecuteQuery(Q_(R"(
                            UPSERT INTO `/Root/Test` (Group, Name, Comment, Amount)
                            VALUES (1U, "Paul", "Changed Other", 100u);
                        )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
                    });

                    {
                        TDispatchOptions opts2;
                        opts2.FinalEvents.emplace_back([&](IEventHandle&) {
                            return evWriteCounter >= 1;
                        });
                        runtime.DispatchEvents(opts2);
                        UNIT_ASSERT(evWriteCounter >= 1);
                    }

                    auto result2 = runtime.WaitFuture(future2);
                    UNIT_ASSERT_VALUES_EQUAL_C(result2.GetStatus(), EStatus::SUCCESS, result2.GetIssues().ToString());
                }

                // Session1 reads again within the same transaction and should see the committed changes
                // This demonstrates that Read Committed sees the latest committed data
                {
                    Cerr << "TEST >> READ 2 --- " << Endl;
                    //TEST: TEvCreateSnapshotRequest, EvRead, TEvCreateSnapshotRequest, EvRead
                    size_t evCreateSnapshotBefore = evCreateSnapshotCounter;
                    size_t evReadBefore = evReadCounter;

                    auto future3 = Kikimr->RunInThreadPool([&] {
                        return session1.ExecuteQuery(Q_(R"(
                            SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;

                            SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
                        )"), TTxControl::Tx(*tx1)).ExtractValueSync();
                    });

                    {
                        TDispatchOptions opts3;
                        opts3.FinalEvents.emplace_back([&](IEventHandle&) {
                            return (evCreateSnapshotCounter - evCreateSnapshotBefore) >= 2 && (evReadCounter - evReadBefore) >= 2;
                        });
                        runtime.DispatchEvents(opts3);
                        UNIT_ASSERT((evCreateSnapshotCounter - evCreateSnapshotBefore) >= 2);
                        UNIT_ASSERT((evReadCounter - evReadBefore) >= 2);
                    }

                    auto result2 = runtime.WaitFuture(future3);
                    UNIT_ASSERT_VALUES_EQUAL_C(result2.GetStatus(), EStatus::SUCCESS, result2.GetIssues().ToString());
                    CompareYson(R"([[[100u];["Changed Other"];1u;"Paul"]])", FormatResultSetYson(result2.GetResultSet(0)));
                    CompareYson(R"([[[100u];["Changed Other"];1u;"Paul"]])", FormatResultSetYson(result2.GetResultSet(1)));

                    Cerr << "TEST >> READ 2 --- FINISH " << Endl;
                }

                // Commit the transaction
                {
                    auto future4 = Kikimr->RunInThreadPool([&] {
                        return tx1->Commit().ExtractValueSync();
                    });
                    auto result2 = runtime.WaitFuture(future4);
                    UNIT_ASSERT_VALUES_EQUAL_C(result2.GetStatus(), EStatus::SUCCESS, result2.GetIssues().ToString());
                }
            }

            // Verify the final state
            {
                auto future5 = Kikimr->RunInThreadPool([&] {
                    return session1.ExecuteQuery(Q_(R"(
                        SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
                    )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW()).CommitTx()).ExtractValueSync();
                });
                auto result = runtime.WaitFuture(future5);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[[100u];["Changed Other"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(TReadSeesLastCommittedOltp) {
        TReadSeesLastCommitted tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(false);
        tester.Execute();
    }

    class TReadSeesOwnChanges : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            // Session1 starts a Read Committed transaction
            {
                auto result = session1.ExecuteQuery(Q_(R"(
                    PRAGMA ydb.DefaultTxMode="ReadCommittedRW";
                    SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
                )"), TTxControl::BeginTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[[300u];["None"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
                auto tx1 = result.GetTransaction();
                UNIT_ASSERT(tx1);

                // Session2 commits first change
                {
                    auto result2 = session2.ExecuteQuery(Q_(R"(
                        UPSERT INTO `/Root/Test` (Group, Name, Comment, Amount)
                        VALUES (1U, "Paul", "First Change", 100u);
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL_C(result2.GetStatus(), EStatus::SUCCESS, result2.GetIssues().ToString());
                }

                // Session1 reads and sees first committed change
                {
                    auto result2 = session1.ExecuteQuery(Q_(R"(
                        PRAGMA ydb.DefaultTxMode="ReadCommittedRW";
                        SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
                    )"), TTxControl::Tx(*tx1)).ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL_C(result2.GetStatus(), EStatus::SUCCESS, result2.GetIssues().ToString());
                    CompareYson(R"([[[100u];["First Change"];1u;"Paul"]])", FormatResultSetYson(result2.GetResultSet(0)));
                }

                // Session1 writes second change
                {
                    auto result2 = session1.ExecuteQuery(Q_(R"(
                        UPSERT INTO `/Root/Test` (Group, Name, Comment, Amount)
                        VALUES (1U, "Paul", "Second Change", 200u);
                    )"), TTxControl::Tx(*tx1)).ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL_C(result2.GetStatus(), EStatus::SUCCESS, result2.GetIssues().ToString());
                }

                // Session1 reads and sees second committed change
                {
                    auto result2 = session1.ExecuteQuery(Q_(R"(
                        PRAGMA ydb.DefaultTxMode="ReadCommittedRW";
                        SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
                    )"), TTxControl::Tx(*tx1)).ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL_C(result2.GetStatus(), EStatus::SUCCESS, result2.GetIssues().ToString());
                    CompareYson(R"([[[200u];["Second Change"];1u;"Paul"]])", FormatResultSetYson(result2.GetResultSet(0)));
                }

                // Commit the transaction
                {
                    auto result2 = tx1->Commit().ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL_C(result2.GetStatus(), EStatus::SUCCESS, result2.GetIssues().ToString());
                }
            }

            // Verify the final state
            {
                auto result = session1.ExecuteQuery(Q_(R"(
                    SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[[200u];["Second Change"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(TReadSeesOwnChangesOltp) {
        TReadSeesOwnChanges tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    class TReadDoesNotSeeUncommitted : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            // Session1 starts a Read Committed transaction
            {
                auto result = session1.ExecuteQuery(Q_(R"(
                    PRAGMA ydb.DefaultTxMode="ReadCommittedRW";
                    SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
                )"), TTxControl::BeginTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[[300u];["None"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
                auto tx1 = result.GetTransaction();
                UNIT_ASSERT(tx1);

                // Session2 starts a transaction but does not commit
                auto result2 = session2.ExecuteQuery(Q_(R"(
                    UPSERT INTO `/Root/Test` (Group, Name, Comment, Amount)
                    VALUES (1U, "Paul", "Uncommitted Change", 100u);
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW())).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result2.GetStatus(), EStatus::SUCCESS, result2.GetIssues().ToString());
                auto tx2 = result2.GetTransaction();
                UNIT_ASSERT(tx2);

                // Session1 reads and should NOT see the uncommitted change
                {
                    auto result3 = session1.ExecuteQuery(Q_(R"(
                        PRAGMA ydb.DefaultTxMode="ReadCommittedRW";
                        SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
                    )"), TTxControl::Tx(*tx1)).ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL_C(result3.GetStatus(), EStatus::SUCCESS, result3.GetIssues().ToString());
                    CompareYson(R"([[[300u];["None"];1u;"Paul"]])", FormatResultSetYson(result3.GetResultSet(0)));
                }

                // Session2 commits
                {
                    auto result3 = tx2->Commit().ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL_C(result3.GetStatus(), EStatus::SUCCESS, result3.GetIssues().ToString());
                }

                // Session1 reads again and now sees the committed change
                {
                    auto result3 = session1.ExecuteQuery(Q_(R"(
                        PRAGMA ydb.DefaultTxMode="ReadCommittedRW";
                        SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
                    )"), TTxControl::Tx(*tx1)).ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL_C(result3.GetStatus(), EStatus::SUCCESS, result3.GetIssues().ToString());
                    CompareYson(R"([[[100u];["Uncommitted Change"];1u;"Paul"]])", FormatResultSetYson(result3.GetResultSet(0)));
                }

                // Commit the transaction
                {
                    auto result3 = tx1->Commit().ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL_C(result3.GetStatus(), EStatus::SUCCESS, result3.GetIssues().ToString());
                }
            }

            // Verify the final state
            {
                auto result = session1.ExecuteQuery(Q_(R"(
                    SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
                )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[[100u];["Uncommitted Change"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(TReadDoesNotSeeUncommittedOltp) {
        TReadDoesNotSeeUncommitted tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    class TUpdateWhereReadCommittedRWNotSupported : public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();
            auto session = client.GetSession().GetValueSync().GetSession();

            auto result = session.ExecuteQuery(Q_(R"(
                UPDATE `/Root/Test` SET Comment = "Updated" WHERE Name == "Paul"
            )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW()).CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::INTERNAL_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "UPDATE WHERE is not supported with READ_COMMITTED_RW");
        }
    };

    Y_UNIT_TEST(TUpdateWhereReadCommittedRWNotSupported) {
        TUpdateWhereReadCommittedRWNotSupported tester;
        tester.SetIsOlap(false);
        tester.SetUseRealThreads(true);
        tester.Execute();
    }
}

} // namespace NKqp
} // namespace NKikimr
