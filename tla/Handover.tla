------------------------------ MODULE Handover ------------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS Files, Txns

FileTxns == [
    f \in Files |->
        IF f = "F1" THEN <<1, 2, 3>> ELSE <<3, 4, 5>>
]

LiveTxns == <<4, 5, 6>>

VARIABLES
    mode,           \* {"Archive", "Live"}
    currentFile,    \* {"F1", "F2", "None"}
    fileCursor,     \* [f \in Files |-> Nat]
    liveCursor,     \* Nat
    appliedSet,     \* SubSet(Txns)
    appliedOrder    \* Seq(Txns)

vars == <<mode, currentFile, fileCursor, liveCursor, appliedSet, appliedOrder>>

TypeOK ==
    /\ mode \in {"Archive", "Live"}
    /\ currentFile \in Files \cup {"None"}
    /\ \A f \in Files : fileCursor[f] \in 1..(Len(FileTxns[f]) + 1)
    /\ liveCursor \in 1..(Len(LiveTxns) + 1)
    /\ appliedSet \subseteq Txns
    /\ \A i \in 1..Len(appliedOrder) : appliedOrder[i] \in Txns

Init ==
    /\ mode = "Archive"
    /\ currentFile = "F1"
    /\ fileCursor = [f \in Files |-> 1]
    /\ liveCursor = 1
    /\ appliedSet = {}
    /\ appliedOrder = <<>>

(* Đọc transaction từ file archive hiện tại *)
ReadArchive(f) ==
    /\ mode = "Archive"
    /\ currentFile = f
    /\ fileCursor[f] <= Len(FileTxns[f])
    /\ LET t == FileTxns[f][fileCursor[f]] IN
       /\ IF t \in appliedSet
          THEN \* Bỏ qua Txn đã có trong appliedSet (không trùng lặp)
               UNCHANGED <<appliedSet, appliedOrder>>
          ELSE \* Apply Txn mới vào target
               /\ appliedSet' = appliedSet \cup {t}
               /\ appliedOrder' = Append(appliedOrder, t)
       /\ fileCursor' = [fileCursor EXCEPT ![f] = @ + 1]
    /\ UNCHANGED <<mode, currentFile, liveCursor>>

(* Chuyển sang file archive tiếp theo *)
SwitchFile ==
    /\ mode = "Archive"
    /\ currentFile = "F1"
    /\ fileCursor["F1"] > Len(FileTxns["F1"])
    /\ currentFile' = "F2"
    /\ UNCHANGED <<mode, fileCursor, liveCursor, appliedSet, appliedOrder>>

(* Handover: hết file archive, chuyển sang đọc live stream *)
HandoverToLive ==
    /\ mode = "Archive"
    /\ currentFile = "F2"
    /\ fileCursor["F2"] > Len(FileTxns["F2"])
    /\ mode' = "Live"
    /\ currentFile' = "None"
    /\ UNCHANGED <<fileCursor, liveCursor, appliedSet, appliedOrder>>

(* Đọc transaction từ live stream *)
ReadLive ==
    /\ mode = "Live"
    /\ liveCursor <= Len(LiveTxns)
    /\ LET t == LiveTxns[liveCursor] IN
       /\ IF t \in appliedSet
          THEN \* Dedup dựa trên appliedSet
               UNCHANGED <<appliedSet, appliedOrder>>
          ELSE /\ appliedSet' = appliedSet \cup {t}
               /\ appliedOrder' = Append(appliedOrder, t)
       /\ liveCursor' = liveCursor + 1
    /\ UNCHANGED <<mode, currentFile, fileCursor>>

Next ==
    \/ \E f \in Files : ReadArchive(f)
    \/ SwitchFile
    \/ HandoverToLive
    \/ ReadLive

-----------------------------------------------------------------------------
(* Bất biến HO1: S02 §12 Handover *)
(* Archive sang live stream: không trùng (no duplicates), không hổng (no gaps) *)
HO1 ==
    /\ \A i, j \in 1..Len(appliedOrder) : (i # j) => (appliedOrder[i] # appliedOrder[j])
    /\ \A i \in 1..Len(appliedOrder) : appliedOrder[i] = i

Spec == Init /\ [][Next]_vars

=============================================================================
