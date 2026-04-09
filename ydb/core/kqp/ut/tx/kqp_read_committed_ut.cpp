#include "kqp_sink_common.h"

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
            auto client = Kikimr->GetQueryClient();
            auto session1 = client.GetSession().GetValueSync().GetSession();
            auto session2 = client.GetSession().GetValueSync().GetSession();

            // Session1 starts a Read Committed transaction and reads initial data
            {
                auto result = session1.ExecuteQuery(Q_(R"(
                    SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
                )"), TTxControl::BeginTx(TTxSettings::ReadCommittedRW())).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[[300u];["None"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
                auto tx1 = result.GetTransaction();
                UNIT_ASSERT(tx1);

                // Session2 commits changes to the same data
                {
                    auto result2 = session2.ExecuteQuery(Q_(R"(
                        UPSERT INTO `/Root/Test` (Group, Name, Comment, Amount)
                        VALUES (1U, "Paul", "Changed Other", 100u);
                    )"), TTxControl::BeginTx(TTxSettings::SnapshotRW()).CommitTx()).ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL_C(result2.GetStatus(), EStatus::SUCCESS, result2.GetIssues().ToString());
                }

                // Session1 reads again within the same transaction and should see the committed changes
                // This demonstrates that Read Committed sees the latest committed data
                {
                    auto result2 = session1.ExecuteQuery(Q_(R"(
                        SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;

                        SELECT * FROM `/Root/Test` WHERE Name == "Paul" ORDER BY Group, Name;
                    )"), TTxControl::Tx(*tx1)).ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL_C(result2.GetStatus(), EStatus::SUCCESS, result2.GetIssues().ToString());
                    CompareYson(R"([[[100u];["Changed Other"];1u;"Paul"]])", FormatResultSetYson(result2.GetResultSet(0)));
                    CompareYson(R"([[[100u];["Changed Other"];1u;"Paul"]])", FormatResultSetYson(result2.GetResultSet(1)));
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
                CompareYson(R"([[[100u];["Changed Other"];1u;"Paul"]])", FormatResultSetYson(result.GetResultSet(0)));
            }
        }
    };

    Y_UNIT_TEST(TReadSeesLastCommittedOltp) {
        TReadSeesLastCommitted tester;
        tester.SetIsOlap(false);
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
}

} // namespace NKqp
} // namespace NKikimr
