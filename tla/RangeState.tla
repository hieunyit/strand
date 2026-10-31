----------------------------- MODULE RangeState -----------------------------
EXTENDS Integers, FiniteSets

CONSTANTS Ranges, Keys, MaxEvents

VARIABLES
    rangeState,     \* [r \in Ranges |-> {"Pending", "Snapshotting", "Streaming"}]
    sourceVer,      \* [k \in Keys |-> Nat]
    snapBuf,        \* [r \in Ranges |-> [k \in Keys |-> Nat]]
    streamBuf,      \* [r \in Ranges |-> [k \in Keys |-> Nat]]
    targetVer,      \* [k \in Keys |-> Nat]
    maxSeenVer,     \* [k \in Keys |-> Nat]: phiên bản cao nhất target từng hiển thị
    eventCount      \* 0..MaxEvents

vars == <<rangeState, sourceVer, snapBuf, streamBuf, targetVer, maxSeenVer, eventCount>>

RangeOf(k) ==
    IF k <= 2 THEN "R1" ELSE "R2"

TypeOK ==
    /\ \A r \in Ranges : rangeState[r] \in {"Pending", "Snapshotting", "Streaming"}
    /\ \A k \in Keys : sourceVer[k] \in 0..10
    /\ \A k \in Keys : targetVer[k] \in 0..10
    /\ \A k \in Keys : maxSeenVer[k] \in 0..10
    /\ eventCount \in 0..MaxEvents

Init ==
    /\ rangeState = [r \in Ranges |-> "Pending"]
    /\ sourceVer = [k \in Keys |-> 1]
    /\ snapBuf = [r \in Ranges |-> [k \in Keys |-> 0]]
    /\ streamBuf = [r \in Ranges |-> [k \in Keys |-> 0]]
    /\ targetVer = [k \in Keys |-> 0]
    /\ maxSeenVer = [k \in Keys |-> 0]
    /\ eventCount = 0

StartSnapshot(r) ==
    /\ rangeState[r] = "Pending"
    /\ rangeState' = [rangeState EXCEPT ![r] = "Snapshotting"]
    /\ snapBuf' = [snapBuf EXCEPT ![r] = [k \in Keys |-> IF RangeOf(k) = r THEN sourceVer[k] ELSE 0]]
    /\ UNCHANGED <<sourceVer, streamBuf, targetVer, maxSeenVer, eventCount>>

StreamUpdate(k) ==
    /\ eventCount < MaxEvents
    /\ sourceVer[k] > 0
    /\ LET newVer == sourceVer[k] + 1
           r == RangeOf(k) IN
       /\ sourceVer' = [sourceVer EXCEPT ![k] = newVer]
       /\ eventCount' = eventCount + 1
       /\ IF rangeState[r] = "Snapshotting"
          THEN /\ streamBuf' = [streamBuf EXCEPT ![r][k] = newVer]
               /\ UNCHANGED <<targetVer, maxSeenVer>>
          ELSE IF rangeState[r] = "Streaming"
               THEN /\ targetVer' = [targetVer EXCEPT ![k] = newVer]
                    /\ maxSeenVer' = [maxSeenVer EXCEPT ![k] = newVer]
                    /\ UNCHANGED streamBuf
               ELSE UNCHANGED <<streamBuf, targetVer, maxSeenVer>>
    /\ UNCHANGED <<rangeState, snapBuf>>

StreamChangePK(kFrom, kTo) ==
    /\ eventCount < MaxEvents
    /\ RangeOf(kFrom) # RangeOf(kTo)
    /\ sourceVer[kFrom] > 0
    /\ sourceVer[kTo] = 0
    /\ LET newVer == sourceVer[kFrom] + 1
           rFrom == RangeOf(kFrom)
           rTo == RangeOf(kTo) IN
       /\ sourceVer' = [sourceVer EXCEPT ![kFrom] = 0, ![kTo] = newVer]
       /\ eventCount' = eventCount + 1
       /\ streamBuf' = [r \in Ranges |->
            [k \in Keys |->
                IF r = rFrom /\ k = kFrom /\ rangeState[rFrom] = "Snapshotting"
                THEN 0
                ELSE IF r = rTo /\ k = kTo /\ rangeState[rTo] = "Snapshotting"
                THEN newVer
                ELSE streamBuf[r][k]]]
       /\ targetVer' = [k \in Keys |->
            IF k = kFrom /\ rangeState[rFrom] = "Streaming"
            THEN 0
            ELSE IF k = kTo /\ rangeState[rTo] = "Streaming"
            THEN newVer
            ELSE targetVer[k]]
       /\ maxSeenVer' = [k \in Keys |->
            IF k = kTo /\ rangeState[rTo] = "Streaming" /\ newVer > maxSeenVer[kTo]
            THEN newVer
            ELSE maxSeenVer[k]]
    /\ UNCHANGED <<rangeState, snapBuf>>

(* Merge và Flush trong cùng 1 transaction: không bao giờ lùi về trạng thái cũ hơn *)
MergeAndFlush(r) ==
    /\ rangeState[r] = "Snapshotting"
    /\ rangeState' = [rangeState EXCEPT ![r] = "Streaming"]
    /\ targetVer' = [k \in Keys |->
            IF RangeOf(k) = r
            THEN LET eff == (IF streamBuf[r][k] > 0 THEN streamBuf[r][k] ELSE snapBuf[r][k]) IN
                 IF targetVer[k] > eff THEN targetVer[k] ELSE eff
            ELSE targetVer[k]]
    /\ maxSeenVer' = [k \in Keys |->
            IF RangeOf(k) = r
            THEN LET eff == (IF streamBuf[r][k] > 0 THEN streamBuf[r][k] ELSE snapBuf[r][k]) IN
                 IF eff > maxSeenVer[k] THEN eff ELSE maxSeenVer[k]
            ELSE maxSeenVer[k]]
    /\ UNCHANGED <<sourceVer, snapBuf, streamBuf, eventCount>>

Next ==
    \/ \E r \in Ranges : StartSnapshot(r)
    \/ \E k \in Keys : StreamUpdate(k)
    \/ \E k1, k2 \in Keys : (RangeOf(k1) # RangeOf(k2)) /\ StreamChangePK(k1, k2)
    \/ \E r \in Ranges : MergeAndFlush(r)

-----------------------------------------------------------------------------
(* Bất biến RS1: S02 §2 (RS-1), §12 *)
(* Target không bao giờ hiển thị một row cũ hơn trạng thái nó đã có *)
RS1 ==
    /\ \A k \in Keys :
        (targetVer[k] > 0 /\ maxSeenVer[k] > 0) => (targetVer[k] >= maxSeenVer[k])
    /\ \A k \in Keys :
        (rangeState[RangeOf(k)] = "Streaming" /\ streamBuf[RangeOf(k)][k] > 0) =>
            (targetVer[k] >= streamBuf[RangeOf(k)][k])

Spec == Init /\ [][Next]_vars

=============================================================================
