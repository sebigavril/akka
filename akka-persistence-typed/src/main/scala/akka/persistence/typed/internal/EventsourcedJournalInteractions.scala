/**
 * Copyright (C) 2016-2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.internal

import akka.actor.ActorRef
import akka.actor.typed.{ Behavior, PostStop, PreRestart, Signal }
import akka.actor.typed.scaladsl.{ ActorContext, Behaviors }
import akka.annotation.InternalApi
import akka.persistence.JournalProtocol.ReplayMessages
import akka.persistence.SnapshotProtocol.LoadSnapshot
import akka.persistence._
import akka.persistence.typed.internal.EventsourcedBehavior.InternalProtocol

import scala.collection.immutable

/** INTERNAL API */
@InternalApi
private[akka] trait EventsourcedJournalInteractions[C, E, S] {

  def setup: EventsourcedSetup[C, E, S]

  type EventOrTagged = Any // `Any` since can be `E` or `Tagged`

  // ---------- journal interactions ---------

  protected def internalPersist(
    state: EventsourcedRunning.EventsourcedState[S],
    event: EventOrTagged): EventsourcedRunning.EventsourcedState[S] = {

    val newState = state.nextSequenceNr()

    val senderNotKnownBecauseAkkaTyped = null
    val repr = PersistentRepr(
      event,
      persistenceId = setup.persistenceId.id,
      sequenceNr = newState.seqNr,
      writerUuid = setup.writerIdentity.writerUuid,
      sender = senderNotKnownBecauseAkkaTyped
    )

    val write = AtomicWrite(repr) :: Nil
    setup.journal.tell(JournalProtocol.WriteMessages(write, setup.selfUntyped, setup.writerIdentity.instanceId), setup.selfUntyped)

    newState
  }

  protected def internalPersistAll(
    events: immutable.Seq[EventOrTagged],
    state:  EventsourcedRunning.EventsourcedState[S]): EventsourcedRunning.EventsourcedState[S] = {
    if (events.nonEmpty) {
      var newState = state

      val writes = events.map { event ⇒
        newState = newState.nextSequenceNr()
        PersistentRepr(
          event,
          persistenceId = setup.persistenceId.id,
          sequenceNr = newState.seqNr,
          writerUuid = setup.writerIdentity.writerUuid,
          sender = ActorRef.noSender)
      }
      val write = AtomicWrite(writes)

      setup.journal.tell(JournalProtocol.WriteMessages(write :: Nil, setup.selfUntyped, setup.writerIdentity.instanceId), setup.selfUntyped)

      newState
    } else state
  }

  protected def replayEvents(fromSeqNr: Long, toSeqNr: Long): Unit = {
    setup.log.debug("Replaying messages: from: {}, to: {}", fromSeqNr, toSeqNr)
    setup.journal ! ReplayMessages(fromSeqNr, toSeqNr, setup.recovery.replayMax, setup.persistenceId.id, setup.selfUntyped)
  }

  protected def requestRecoveryPermit(): Unit = {
    setup.persistence.recoveryPermitter.tell(RecoveryPermitter.RequestRecoveryPermit, setup.selfUntyped)
  }

  /** Intended to be used in .onSignal(returnPermitOnStop) by behaviors */
  protected def returnPermitOnStop: PartialFunction[(ActorContext[InternalProtocol], Signal), Behavior[InternalProtocol]] = {
    case (_, PostStop) ⇒
      tryReturnRecoveryPermit("PostStop")
      Behaviors.stopped
    case (_, PreRestart) ⇒
      tryReturnRecoveryPermit("PreRestart")
      Behaviors.stopped
  }

  /** Mutates setup, by setting the `holdingRecoveryPermit` to false */
  protected def tryReturnRecoveryPermit(reason: String): Unit = {
    if (setup.holdingRecoveryPermit) {
      setup.log.debug("Returning recovery permit, reason: {}", reason)
      setup.persistence.recoveryPermitter.tell(RecoveryPermitter.ReturnRecoveryPermit, setup.selfUntyped)
      setup.holdingRecoveryPermit = false
    } // else, no need to return the permit
  }

  // ---------- snapshot store interactions ---------

  /**
   * Instructs the snapshot store to load the specified snapshot and send it via an [[SnapshotOffer]]
   * to the running [[PersistentActor]].
   */
  protected def loadSnapshot(criteria: SnapshotSelectionCriteria, toSequenceNr: Long): Unit = {
    setup.snapshotStore.tell(LoadSnapshot(setup.persistenceId.id, criteria, toSequenceNr), setup.selfUntyped)
  }

  protected def internalSaveSnapshot(state: EventsourcedRunning.EventsourcedState[S]): Unit = {
    // don't store null state
    if (state.state != null)
      setup.snapshotStore.tell(SnapshotProtocol.SaveSnapshot(
        SnapshotMetadata(setup.persistenceId.id, state.seqNr),
        state.state), setup.selfUntyped)
  }

}
