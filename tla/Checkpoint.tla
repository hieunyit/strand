----------------------------- MODULE Checkpoint -----------------------------
EXTENDS Integers, FiniteSets

CONSTANTS Workers, Txns

VARIABLES
    workerDelta,        \* [w \in Workers |-> SubSet(Txns)]
    inFlight,           \* [w \in Workers |-> Nat]: Txn đang được worker xử lý (0 nếu rảnh)
    nextTxn,            \* Nat: Txn tiếp theo cần dispatch (1..Cardinality(Txns)+1)
    globalApplied,      \* SubSet(Txns)
    targetData,         \* SubSet(Txns): các Txn có dữ liệu đã commit ở target
    crashed,            \* BOOLEAN
    recovered,          \* BOOLEAN
    recoveredSet,       \* SubSet(Txns)
    committedAtCrash,   \* SubSet(Txns): snapshot targetData tại thời điểm crash
    applyCount          \* [t \in Txns |-> Nat]

vars == <<workerDelta, inFlight, nextTxn, globalApplied, targetData, crashed, recovered, recoveredSet, committedAtCrash, applyCount>>

MaxTxn == Cardinality(Txns)

TypeOK ==
    /\ \A w \in Workers : workerDelta[w] \subseteq Txns
    /\ \A w \in Workers : inFlight[w] \in 0..MaxTxn
    /\ nextTxn \in 1..(MaxTxn + 1)
    /\ globalApplied \subseteq Txns
    /\ targetData \subseteq Txns
    /\ crashed \in BOOLEAN
    /\ recovered \in BOOLEAN
    /\ recoveredSet \subseteq Txns
    /\ committedAtCrash \subseteq Txns
    /\ \A t \in Txns : applyCount[t] \in 0..2

Init ==
    /\ workerDelta = [w \in Workers |-> {}]
    /\ inFlight = [w \in Workers |-> 0]
    /\ nextTxn = 1
    /\ globalApplied = {}
    /\ targetData = {}
    /\ crashed = FALSE
    /\ recovered = FALSE
    /\ recoveredSet = {}
    /\ committedAtCrash = {}
    /\ applyCount = [t \in Txns |-> 0]

(* Dispatch: scheduler giao Txn tiếp theo cho worker đang rảnh *)
Dispatch(w) ==
    /\ ~crashed
    /\ inFlight[w] = 0
    /\ nextTxn <= MaxTxn
    /\ inFlight' = [inFlight EXCEPT ![w] = nextTxn]
    /\ nextTxn' = nextTxn + 1
    /\ UNCHANGED <<workerDelta, globalApplied, targetData, crashed, recovered, recoveredSet, committedAtCrash, applyCount>>

(* Worker commit: trong cùng 1 transaction, dữ liệu ghi vào targetData và row delta cập nhật *)
WorkerCommit(w) ==
    /\ ~crashed
    /\ inFlight[w] # 0
    /\ LET t == inFlight[w] IN
       /\ targetData' = targetData \cup {t}
       /\ workerDelta' = [workerDelta EXCEPT ![w] = @ \cup {t}]
       /\ inFlight' = [inFlight EXCEPT ![w] = 0]
       /\ applyCount' = [applyCount EXCEPT ![t] = @ + 1]
    /\ UNCHANGED <<nextTxn, globalApplied, crashed, recovered, recoveredSet, committedAtCrash>>

(* Compaction: Coordinator gộp mọi row delta vào globalApplied rồi dọn dẹp row delta *)
Compaction ==
    /\ ~crashed
    /\ LET allDeltas == UNION {workerDelta[w] : w \in Workers} IN
       /\ allDeltas # {}
       /\ globalApplied' = globalApplied \cup allDeltas
       /\ workerDelta' = [w \in Workers |-> {}]
       /\ UNCHANGED <<inFlight, nextTxn, targetData, crashed, recovered, recoveredSet, committedAtCrash, applyCount>>

(* Crash: xảy ra bất kỳ lúc nào khi chưa crash *)
Crash ==
    /\ ~crashed
    /\ ~recovered
    /\ crashed' = TRUE
    /\ committedAtCrash' = targetData
    /\ inFlight' = [w \in Workers |-> 0]
    /\ UNCHANGED <<workerDelta, nextTxn, globalApplied, targetData, recovered, recoveredSet, applyCount>>

(* Recover: nạp lại checkpoint từ globalApplied và các worker delta trên storage *)
Recover ==
    /\ crashed
    /\ ~recovered
    /\ LET stored == globalApplied \cup (UNION {workerDelta[w] : w \in Workers}) IN
       /\ recoveredSet' = stored
       /\ recovered' = TRUE
       /\ crashed' = FALSE
       /\ UNCHANGED <<workerDelta, inFlight, nextTxn, globalApplied, targetData, committedAtCrash, applyCount>>

Next ==
    \/ \E w \in Workers : Dispatch(w)
    \/ \E w \in Workers : WorkerCommit(w)
    \/ Compaction
    \/ Crash
    \/ Recover

-----------------------------------------------------------------------------
(* Bất biến CP1: S02 §2 (CP-1), §12 *)
(* Khi restart, tập đã apply chứa mọi Txn mà dữ liệu đã commit ở target *)
CP1 ==
    /\ \A t \in targetData : t \in (globalApplied \cup (UNION {workerDelta[w] : w \in Workers}))
    /\ (recovered => (\A t \in committedAtCrash : t \in recoveredSet))

(* Bất biến CP2: S02 §2 (CP-2), §12 *)
(* Không Txn nào được apply hai lần *)
CP2 ==
    \A t \in Txns : applyCount[t] <= 1

(* Bất biến CP4: S02 §2 (CP-4), §12 *)
(* Giới hạn delta và uncompacted holes *)
CP4 ==
    \A w \in Workers : Cardinality(workerDelta[w]) <= Cardinality(Txns)

Spec == Init /\ [][Next]_vars

=============================================================================
