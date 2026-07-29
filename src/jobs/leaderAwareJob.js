'use strict';

/**
 * Leader-aware job wrapper.
 *
 * Wraps a job module (e.g. priceRefresh, webhookRetryWorker, airdropExpiry)
 * with leader-election coordination so that the underlying job's scheduled
 * work only actually executes on the instance that currently holds the
 * Redis-based leader lease.
 *
 * Non-leader instances remain ready to take over if the current leader's
 * lease expires. Leadership state transitions are logged clearly for
 * debugging in production.
 *
 * Usage:
 *   const leaderElection = require('../services/leaderElection');
 *   const priceRefreshJob = require('./priceRefresh');
 *   const wrappedJob = makeLeaderAwareJob({
 *     job: priceRefreshJob,
 *     jobName: 'price_refresh',
 *     leaderElection: leaderElection.createLeaderElection('price_refresh'),
 *     logger: require('../logger'),
 *   });
 *   wrappedJob.start();  // only starts underlying job if leader
 *   wrappedJob.stop();   // stops underlying job and releases lease
 *   wrappedJob.getHealth(); // includes leadership info
 */

function makeLeaderAwareJob({ job, jobName, leaderElection, logger }) {
  let underlyingStarted = false;
  let manualStop = false;
  let leadershipLostWhileRunning = false;

  /**
   * Handle acquiring leadership: start the underlying job.
   */
  function onLeadershipAcquired() {
    if (manualStop) return;
    if (!underlyingStarted) {
      logger.info('Acting as leader — starting scheduled job', { job: jobName });
      job.start();
      underlyingStarted = true;
    } else if (leadershipLostWhileRunning) {
      // We lost leadership briefly and regained it — the underlying job was
      // stopped when we lost it, so restart it.
      logger.info('Re-acquired leadership — restarting scheduled job', { job: jobName });
      job.start();
      leadershipLostWhileRunning = false;
    }
  }

  /**
   * Handle losing leadership: stop the underlying job immediately.
   */
  function onLeadershipLost() {
    if (underlyingStarted) {
      logger.warn('Lost leadership — stopping scheduled job', { job: jobName });
      job.stop();
      underlyingStarted = false;
      leadershipLostWhileRunning = true;
    }
  }

  /**
   * Start the leader-election renewal loop.
   *
   * The underlying job's actual execution (cron / setInterval) is only
   * started when this instance acquires the leader lease. Non-leader
   * instances run only the renewal loop, staying ready to take over.
   */
  function start() {
    manualStop = false;
    leadershipLostWhileRunning = false;

    // Register a callback on the leader election to react to state changes.
    // We wrap the original startRenewLoop to also monitor transitions.
    const origIsLeader = leaderElection.isLeader;
    const origTryAcquire = leaderElection.tryAcquire;
    const origRenew = leaderElection.renew;

    // Patch the leaderElection to notify us on state changes
    let wasLeader = false;

    const checkLeader = () => {
      const isLeaderNow = leaderElection.isLeader();
      if (isLeaderNow && !wasLeader) {
        onLeadershipAcquired();
      } else if (!isLeaderNow && wasLeader) {
        onLeadershipLost();
      }
      wasLeader = isLeaderNow;
    };

    // Override isLeader to include our reactivity
    const originalStartRenewLoop = leaderElection.startRenewLoop.bind(leaderElection);
    const originalStopRenewLoop = leaderElection.stopRenewLoop.bind(leaderElection);

    // Start the renewal loop (which will call tryAcquire immediately)
    leaderElection.startRenewLoop = () => {
      originalStartRenewLoop();

      // Also poll periodically to detect leadership transitions
      // (the renewal loop already does this, but we hook into it)
      logger.info('Leader-aware job started — awaiting leadership', { job: jobName, instanceId: leaderElection.instanceId });
    };

    leaderElection.stopRenewLoop = async () => {
      await originalStopRenewLoop();
      if (underlyingStarted) {
        job.stop();
        underlyingStarted = false;
      }
    };

    // Check leadership state on a short interval to react quickly
    // to transitions detected by the renewal loop
    const checkInterval = setInterval(() => {
      checkLeader();
    }, Math.min(leaderElection.renewIntervalMs || 5000, 2000));

    if (typeof checkInterval.unref === 'function') {
      checkInterval.unref();
    }

    // Store cleanup
    leaderElection._checkInterval = checkInterval;

    // Initial check after a short delay to let the first acquire complete
    setTimeout(() => checkLeader(), 500);

    // Also call startRenewLoop
    leaderElection.startRenewLoop();

    // Patch the tryAcquire to trigger our callback
    const superTryAcquire = leaderElection.tryAcquire;
    leaderElection.tryAcquire = async (...args) => {
      const result = await superTryAcquire(...args);
      checkLeader();
      return result;
    };

    const superRenew = leaderElection.renew;
    leaderElection.renew = async (...args) => {
      const result = await superRenew(...args);
      checkLeader();
      return result;
    };
  }

  /**
   * Stop the leader-election loop and the underlying job.
   */
  async function stop() {
    manualStop = true;
    if (leaderElection._checkInterval) {
      clearInterval(leaderElection._checkInterval);
      leaderElection._checkInterval = null;
    }

    // Release the lease and stop the renewal loop
    await leaderElection.stopRenewLoop();

    if (underlyingStarted) {
      job.stop();
      underlyingStarted = false;
    }

    logger.info('Leader-aware job stopped', { job: jobName });
  }

  /**
   * Returns health info including leadership status.
   */
  function getHealth() {
    const baseHealth = typeof job.getHealth === 'function' ? job.getHealth() : {};
    const leaderState = leaderElection.getState();

    return {
      ...baseHealth,
      leader: leaderState.isLeader,
      leaderInstanceId: leaderState.instanceId,
      leaderSince: leaderState.acquiredAt,
      lockKey: leaderState.lockKey,
    };
  }

  /**
   * Get the underlying leader election instance (useful for tests).
   */
  function getLeaderElection() {
    return leaderElection;
  }

  return {
    start,
    stop,
    getHealth,
    getLeaderElection,
    jobName,
    underlyingJob: job,
  };
}

module.exports = { makeLeaderAwareJob };

