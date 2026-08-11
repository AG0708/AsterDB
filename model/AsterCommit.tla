--------------------------- MODULE AsterCommit ---------------------------
EXTENDS FiniteSets, Naturals, Sequences, TLC

CONSTANTS Nodes, Values, InitialValue, NoNode, MaxLog, MaxTerm, MaxReads

ASSUME /\ Nodes # {}
       /\ Values # {}
       /\ InitialValue \notin Values
       /\ NoNode \notin Nodes
       /\ MaxLog \in Nat \ {0}
       /\ MaxTerm \in Nat \ {0}
       /\ MaxReads \in Nat \ {0}

VARIABLES term,
          leader,
          online,
          connected,
          logs,
          commitIndex,
          applied,
          acknowledged,
          reads

vars == <<term, leader, online, connected, logs, commitIndex,
          applied, acknowledged, reads>>

EntryType == [term : Nat, value : Values]
ReadType == [barrier : 0..MaxLog, value : Values \cup {InitialValue}]

Prefix(sequence, length) ==
    IF length = 0 THEN <<>> ELSE SubSeq(sequence, 1, length)

HasAcknowledgedPrefix(node, length) ==
    /\ length <= Len(logs[node])
    /\ Prefix(logs[node], length) = Prefix(acknowledged, length)

Quorum(members) == 2 * Cardinality(members) > Cardinality(Nodes)

Symmetry == Permutations(Nodes)

ValueAt(index) ==
    IF index = 0 THEN InitialValue ELSE acknowledged[index].value

TypeOK ==
    /\ term \in 0..MaxTerm
    /\ leader \in Nodes \cup {NoNode}
    /\ online \in SUBSET Nodes
    /\ connected \in SUBSET Nodes
    /\ logs \in [Nodes -> Seq(EntryType)]
    /\ \A node \in Nodes : Len(logs[node]) <= MaxLog
    /\ commitIndex \in 0..MaxLog
    /\ applied \in [Nodes -> 0..MaxLog]
    /\ acknowledged \in Seq(EntryType)
    /\ Len(acknowledged) = commitIndex
    /\ reads \in Seq(ReadType)
    /\ Len(reads) <= MaxReads

Init ==
    /\ term = 0
    /\ leader = NoNode
    /\ online = Nodes
    /\ connected = Nodes
    /\ logs = [node \in Nodes |-> <<>>]
    /\ commitIndex = 0
    /\ applied = [node \in Nodes |-> 0]
    /\ acknowledged = <<>>
    /\ reads = <<>>

Elect(node) ==
    /\ node \in online \cap connected
    /\ Quorum(online \cap connected)
    /\ HasAcknowledgedPrefix(node, commitIndex)
    /\ term < MaxTerm
    /\ term' = term + 1
    /\ leader' = node
    /\ UNCHANGED <<online, connected, logs, commitIndex,
                    applied, acknowledged, reads>>

AppendValue(value) ==
    /\ leader \in online \cap connected
    /\ Len(logs[leader]) < MaxLog
    /\ logs' = [logs EXCEPT
                   ![leader] = Append(@, [term |-> term, value |-> value])]
    /\ UNCHANGED <<term, leader, online, connected, commitIndex,
                    applied, acknowledged, reads>>

Replicate(node) ==
    /\ leader \in online \cap connected
    /\ node \in (online \cap connected) \ {leader}
    /\ logs' = [logs EXCEPT ![node] = logs[leader]]
    /\ UNCHANGED <<term, leader, online, connected, commitIndex,
                    applied, acknowledged, reads>>

ReplicasThrough(index) ==
    {node \in Nodes :
        /\ node \in online \cap connected
        /\ index <= Len(logs[node])
        /\ Prefix(logs[node], index) = Prefix(logs[leader], index)}

Commit ==
    /\ leader \in online \cap connected
    /\ commitIndex < Len(logs[leader])
    /\ \E index \in (commitIndex + 1)..Len(logs[leader]) :
         /\ logs[leader][index].term = term
         /\ Quorum(ReplicasThrough(index))
         /\ commitIndex' = index
         /\ acknowledged' =
                acknowledged \o SubSeq(logs[leader], commitIndex + 1, index)
         /\ UNCHANGED <<term, leader, online, connected, logs,
                         applied, reads>>

Apply(node) ==
    /\ node \in online
    /\ applied[node] < commitIndex
    /\ HasAcknowledgedPrefix(node, applied[node] + 1)
    /\ applied' = [applied EXCEPT ![node] = @ + 1]
    /\ UNCHANGED <<term, leader, online, connected, logs, commitIndex,
                    acknowledged, reads>>

ReadIndex ==
    /\ leader \in online \cap connected
    /\ Quorum(online \cap connected)
    /\ applied[leader] = commitIndex
    /\ Len(reads) < MaxReads
    /\ reads' = Append(reads,
                       [barrier |-> commitIndex,
                        value |-> ValueAt(commitIndex)])
    /\ UNCHANGED <<term, leader, online, connected, logs, commitIndex,
                    applied, acknowledged>>

Crash(node) ==
    /\ node \in online
    /\ online' = online \ {node}
    /\ leader' = IF leader = node THEN NoNode ELSE leader
    /\ UNCHANGED <<term, connected, logs, commitIndex,
                    applied, acknowledged, reads>>

Restart(node) ==
    /\ node \in Nodes \ online
    /\ online' = online \cup {node}
    /\ UNCHANGED <<term, leader, connected, logs, commitIndex,
                    applied, acknowledged, reads>>

Partition(node) ==
    /\ node \in connected
    /\ connected' = connected \ {node}
    /\ leader' = IF leader = node THEN NoNode ELSE leader
    /\ UNCHANGED <<term, online, logs, commitIndex,
                    applied, acknowledged, reads>>

Heal(node) ==
    /\ node \in Nodes \ connected
    /\ connected' = connected \cup {node}
    /\ UNCHANGED <<term, leader, online, logs, commitIndex,
                    applied, acknowledged, reads>>

Next ==
    \/ \E node \in Nodes : Elect(node)
    \/ \E value \in Values : AppendValue(value)
    \/ \E node \in Nodes : Replicate(node)
    \/ Commit
    \/ \E node \in Nodes : Apply(node)
    \/ ReadIndex
    \/ \E node \in Nodes : Crash(node)
    \/ \E node \in Nodes : Restart(node)
    \/ \E node \in Nodes : Partition(node)
    \/ \E node \in Nodes : Heal(node)

Spec == Init /\ [][Next]_vars

LeaderContainsAcknowledgedPrefix ==
    leader = NoNode \/ HasAcknowledgedPrefix(leader, commitIndex)

AcknowledgedOnDurableQuorum ==
    Quorum({node \in Nodes : HasAcknowledgedPrefix(node, commitIndex)})

AppliedPrefixIsAcknowledged ==
    \A node \in Nodes :
        /\ applied[node] <= commitIndex
        /\ HasAcknowledgedPrefix(node, applied[node])

ReadIndexSafety ==
    \A index \in 1..Len(reads) :
        reads[index].value = ValueAt(reads[index].barrier)

CommitTimestampIsLogIndex == Len(acknowledged) = commitIndex

=============================================================================
