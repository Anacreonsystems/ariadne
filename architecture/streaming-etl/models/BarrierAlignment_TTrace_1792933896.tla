---- MODULE BarrierAlignment_TTrace_1792933896 ----
EXTENDS Sequences, TLCExt, BarrierAlignment, Toolbox, Naturals, TLC, BarrierAlignment_TEConstants

_expression ==
    LET BarrierAlignment_TEExpression == INSTANCE BarrierAlignment_TEExpression
    IN BarrierAlignment_TEExpression!expression
----

_trace ==
    LET BarrierAlignment_TETrace == INSTANCE BarrierAlignment_TETrace
    IN BarrierAlignment_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        snapshots = ({})
        /\
        cursors = ((left :> 0 @@ right :> 0))
        /\
        ownerEpoch = (1)
        /\
        barriers = ((left :> 0 @@ right :> 0))
        /\
        barrierCuts = ((left :> 0 @@ right :> 0))
        /\
        postBarrierInputs = ({})
        /\
        acceptedWriteEpoch = (2)
        /\
        snapshotStateCuts = (<<(left :> 0 @@ right :> 0), (left :> 0 @@ right :> 0)>>)
        /\
        snapshotBarrierCuts = (<<(left :> 0 @@ right :> 0), (left :> 0 @@ right :> 0)>>)
        /\
        snapshotBarrierIds = (<<(left :> 0 @@ right :> 0), (left :> 0 @@ right :> 0)>>)
        /\
        activeCheckpoint = (0)
        /\
        highWater = (0)
    )
----

_init ==
    /\ snapshotBarrierIds = _TETrace[1].snapshotBarrierIds
    /\ barrierCuts = _TETrace[1].barrierCuts
    /\ activeCheckpoint = _TETrace[1].activeCheckpoint
    /\ acceptedWriteEpoch = _TETrace[1].acceptedWriteEpoch
    /\ cursors = _TETrace[1].cursors
    /\ postBarrierInputs = _TETrace[1].postBarrierInputs
    /\ snapshots = _TETrace[1].snapshots
    /\ snapshotBarrierCuts = _TETrace[1].snapshotBarrierCuts
    /\ snapshotStateCuts = _TETrace[1].snapshotStateCuts
    /\ ownerEpoch = _TETrace[1].ownerEpoch
    /\ barriers = _TETrace[1].barriers
    /\ highWater = _TETrace[1].highWater
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ snapshotBarrierIds  = _TETrace[i].snapshotBarrierIds
        /\ snapshotBarrierIds' = _TETrace[j].snapshotBarrierIds
        /\ barrierCuts  = _TETrace[i].barrierCuts
        /\ barrierCuts' = _TETrace[j].barrierCuts
        /\ activeCheckpoint  = _TETrace[i].activeCheckpoint
        /\ activeCheckpoint' = _TETrace[j].activeCheckpoint
        /\ acceptedWriteEpoch  = _TETrace[i].acceptedWriteEpoch
        /\ acceptedWriteEpoch' = _TETrace[j].acceptedWriteEpoch
        /\ cursors  = _TETrace[i].cursors
        /\ cursors' = _TETrace[j].cursors
        /\ postBarrierInputs  = _TETrace[i].postBarrierInputs
        /\ postBarrierInputs' = _TETrace[j].postBarrierInputs
        /\ snapshots  = _TETrace[i].snapshots
        /\ snapshots' = _TETrace[j].snapshots
        /\ snapshotBarrierCuts  = _TETrace[i].snapshotBarrierCuts
        /\ snapshotBarrierCuts' = _TETrace[j].snapshotBarrierCuts
        /\ snapshotStateCuts  = _TETrace[i].snapshotStateCuts
        /\ snapshotStateCuts' = _TETrace[j].snapshotStateCuts
        /\ ownerEpoch  = _TETrace[i].ownerEpoch
        /\ ownerEpoch' = _TETrace[j].ownerEpoch
        /\ barriers  = _TETrace[i].barriers
        /\ barriers' = _TETrace[j].barriers
        /\ highWater  = _TETrace[i].highWater
        /\ highWater' = _TETrace[j].highWater

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("BarrierAlignment_TTrace_1792933896.json", _TETrace)

=============================================================================

 Note that you can extract this module `BarrierAlignment_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `BarrierAlignment_TEExpression.tla` file takes precedence 
  over the module `BarrierAlignment_TEExpression` below).

---- MODULE BarrierAlignment_TEExpression ----
EXTENDS Sequences, TLCExt, BarrierAlignment, Toolbox, Naturals, TLC, BarrierAlignment_TEConstants

expression == 
    [
        \* To hide variables of the `BarrierAlignment` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        snapshotBarrierIds |-> snapshotBarrierIds
        ,barrierCuts |-> barrierCuts
        ,activeCheckpoint |-> activeCheckpoint
        ,acceptedWriteEpoch |-> acceptedWriteEpoch
        ,cursors |-> cursors
        ,postBarrierInputs |-> postBarrierInputs
        ,snapshots |-> snapshots
        ,snapshotBarrierCuts |-> snapshotBarrierCuts
        ,snapshotStateCuts |-> snapshotStateCuts
        ,ownerEpoch |-> ownerEpoch
        ,barriers |-> barriers
        ,highWater |-> highWater
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_snapshotBarrierIdsUnchanged |-> snapshotBarrierIds = snapshotBarrierIds'
        
        \* Format the `snapshotBarrierIds` variable as Json value.
        \* ,_snapshotBarrierIdsJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(snapshotBarrierIds)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_snapshotBarrierIdsModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].snapshotBarrierIds # _TETrace[s-1].snapshotBarrierIds
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE BarrierAlignment_TETrace ----
\*EXTENDS IOUtils, BarrierAlignment, TLC, BarrierAlignment_TEConstants
\*
\*trace == IODeserialize("BarrierAlignment_TTrace_1792933896.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE BarrierAlignment_TETrace ----
EXTENDS BarrierAlignment, TLC, BarrierAlignment_TEConstants

trace == 
    <<
    ([snapshots |-> {},cursors |-> (left :> 0 @@ right :> 0),ownerEpoch |-> 1,barriers |-> (left :> 0 @@ right :> 0),barrierCuts |-> (left :> 0 @@ right :> 0),postBarrierInputs |-> {},acceptedWriteEpoch |-> 0,snapshotStateCuts |-> <<(left :> 0 @@ right :> 0), (left :> 0 @@ right :> 0)>>,snapshotBarrierCuts |-> <<(left :> 0 @@ right :> 0), (left :> 0 @@ right :> 0)>>,snapshotBarrierIds |-> <<(left :> 0 @@ right :> 0), (left :> 0 @@ right :> 0)>>,activeCheckpoint |-> 0,highWater |-> 0]),
    ([snapshots |-> {},cursors |-> (left :> 0 @@ right :> 0),ownerEpoch |-> 1,barriers |-> (left :> 0 @@ right :> 0),barrierCuts |-> (left :> 0 @@ right :> 0),postBarrierInputs |-> {},acceptedWriteEpoch |-> 2,snapshotStateCuts |-> <<(left :> 0 @@ right :> 0), (left :> 0 @@ right :> 0)>>,snapshotBarrierCuts |-> <<(left :> 0 @@ right :> 0), (left :> 0 @@ right :> 0)>>,snapshotBarrierIds |-> <<(left :> 0 @@ right :> 0), (left :> 0 @@ right :> 0)>>,activeCheckpoint |-> 0,highWater |-> 0])
    >>
----


=============================================================================

---- MODULE BarrierAlignment_TEConstants ----
EXTENDS BarrierAlignment

CONSTANTS left, right

=============================================================================

---- CONFIG BarrierAlignment_TTrace_1792933896 ----
CONSTANTS
    Inputs = { left , right }
    MaxCheckpoint = 2
    MaxEpoch = 2
    MaxPosition = 2
    right = right
    left = left

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Sun Oct 25 13:11:37 UTC 2026