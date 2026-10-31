----------------------------- MODULE Scheduler -----------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS Tables, Txns, Workers

TxnDef == [
    t \in Txns |->
        CASE t = 1 -> [table |-> "T1", keys |-> {"k1"}, isTable |-> FALSE]
          [] t = 2 -> [table |-> "T1", keys |-> {"k2"}, isTable |-> FALSE]
          [] t = 3 -> [table |-> "T1", keys |-> {}, isTable |-> TRUE]
          [] t = 4 -> [table |-> "T1", keys |-> {"k1"}, isTable |-> FALSE]
          [] t = 5 -> [table |-> "T2", keys |-> {"k1"}, isTable |-> FALSE]
          [] t = 6 -> [table |-> "T1", keys |-> {"k2"}, isTable |-> FALSE]
]

VARIABLES admitted, deps, executing, done, commitOrder

vars == <<admitted, deps, executing, done, commitOrder>>

Conflicts(t1, t2) ==
    /\ TxnDef[t1].table = TxnDef[t2].table
    /\ \/ TxnDef[t1].isTable
       \/ TxnDef[t2].isTable
       \/ (TxnDef[t1].keys \cap TxnDef[t2].keys # {})

TypeOK ==
    /\ admitted \subseteq Txns
    /\ done \subseteq admitted
    /\ \A t \in Txns : deps[t] \subseteq Txns
    /\ \A w \in Workers : executing[w] = 0 \/ executing[w] \in Txns
    /\ \A i \in 1..Len(commitOrder) : commitOrder[i] \in Txns

Init ==
    /\ admitted = {}
    /\ deps = [t \in Txns |-> {}]
    /\ executing = [w \in Workers |-> 0]
    /\ done = {}
    /\ commitOrder = <<>>

NextTxnToAdmit ==
    Cardinality(admitted) + 1

Admit ==
    LET t == NextTxnToAdmit IN
    /\ t \in Txns
    /\ deps' = [deps EXCEPT ![t] = {p \in admitted : Conflicts(p, t) /\ p \notin done}]
    /\ admitted' = admitted \cup {t}
    /\ UNCHANGED <<executing, done, commitOrder>>

CurrentlyExecuting ==
    {executing[w] : w \in Workers} \ {0}

Ready(t) ==
    /\ t \in admitted
    /\ t \notin done
    /\ t \notin CurrentlyExecuting
    /\ deps[t] \subseteq done

Start(w, t) ==
    /\ executing[w] = 0
    /\ Ready(t)
    /\ executing' = [executing EXCEPT ![w] = t]
    /\ UNCHANGED <<admitted, deps, done, commitOrder>>

Finish(w) ==
    LET t == executing[w] IN
    /\ t # 0
    /\ executing' = [executing EXCEPT ![w] = 0]
    /\ done' = done \cup {t}
    /\ commitOrder' = Append(commitOrder, t)
    /\ UNCHANGED <<admitted, deps>>

Next ==
    \/ Admit
    \/ \E w \in Workers, t \in Txns : Start(w, t)
    \/ \E w \in Workers : Finish(w)

-----------------------------------------------------------------------------
(* Bất biến CO1: S02 §2 (CO-1), §12 *)
(* Hai Txn có writeset giao nhau commit ở target theo đúng thứ tự source *)
CO1 ==
    \A i, j \in 1..Len(commitOrder) :
        LET t1 == commitOrder[i]
            t2 == commitOrder[j]
        IN (t1 < t2 /\ Conflicts(t1, t2)) => (i < j)

(* Bất biến kiểm tra không thiếu cạnh phụ thuộc đối với Txn cấp bảng *)
NoMissingEdges ==
    \A t \in admitted :
        \A p \in admitted :
            (p < t /\ Conflicts(p, t) /\ p \notin done) => (p \in deps[t] \/ p \in done)

Spec == Init /\ [][Next]_vars

=============================================================================
