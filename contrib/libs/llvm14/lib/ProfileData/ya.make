# Generated by devtools/yamaker.

LIBRARY()

VERSION(14.0.6)

LICENSE(Apache-2.0 WITH LLVM-exception)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PEERDIR(
    contrib/libs/llvm14
    contrib/libs/llvm14/include
    contrib/libs/llvm14/lib/DebugInfo/DWARF
    contrib/libs/llvm14/lib/Demangle
    contrib/libs/llvm14/lib/IR
    contrib/libs/llvm14/lib/Object
    contrib/libs/llvm14/lib/Support
)

ADDINCL(
    contrib/libs/llvm14/lib/ProfileData
)

NO_COMPILER_WARNINGS()

NO_UTIL()

SRCS(
    GCOV.cpp
    InstrProf.cpp
    InstrProfCorrelator.cpp
    InstrProfReader.cpp
    InstrProfWriter.cpp
    ProfileSummaryBuilder.cpp
    RawMemProfReader.cpp
    SampleProf.cpp
    SampleProfReader.cpp
    SampleProfWriter.cpp
)

END()
