--------------------------- MODULE Checkpoint_err ---------------------------
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

Dispatch(w) ==
    /\ ~crashed
    /\ inFlight[w] = 0
    /\ nextTxn <= MaxTxn
    /\ inFlight' = [inFlight EXCEPT ![w] = nextTxn]
    /\ nextTxn' = nextTxn + 1
    /\ UNCHANGED <<workerDelta, globalApplied, targetData, crashed, recovered, recoveredSet, committedAtCrash, applyCount>>

WorkerCommit(w) ==
    /\ ~crashed
    /\ inFlight[w] # 0
    /\ LET t == inFlight[w] IN
       /\ targetData' = targetData \cup {t}
       /\ workerDelta' = [workerDelta EXCEPT ![w] = @ \cup {t}]
       /\ inFlight' = [inFlight EXCEPT ![w] = 0]
       /\ applyCount' = [applyCount EXCEPT ![t] = @ + 1]
    /\ UNCHANGED <<nextTxn, globalApplied, crashed, recovered, recoveredSet, committedAtCrash>>

(* BUG MUTANT: Compaction dọn sạch workerDelta nhưng quên gộp deltas vào globalApplied *)
Compaction ==
    /\ ~crashed
    /\ LET allDeltas == UNION {workerDelta[w] : w \in Workers} IN
       /\ allDeltas # {}
       /\ workerDelta' = [w \in Workers |-> {}]
       /\ UNCHANGED <<globalApplied, inFlight, nextTxn, targetData, crashed, recovered, recoveredSet, committedAtCrash, applyCount>>

Crash ==
    /\ ~crashed
    /\ ~recovered
    /\ crashed' = TRUE
    /\ committedAtCrash' = targetData
    /\ inFlight' = [w \in Workers |-> 0]
    /\ UNCHANGED <<workerDelta, nextTxn, globalApplied, targetData, recovered, recoveredSet, applyCount>>

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

(* Bất biến CP1: S02 §2 (CP-1), §12 *)
CP1 ==
    /\ \A t \in targetData : t \in (globalApplied \cup (UNION {workerDelta[w] : w \in Workers}))
    /\ (recovered => (\A t \in committedAtCrash : t \in recoveredSet))

Spec == Init /\ [][Next]_vars

=============================================================================
