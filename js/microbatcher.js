/**
 * job order will be preserved so long as the batchProcessor preserves the order.
 */
class MicroBatcher {
  #jobQueue;
  #timer;
  #isProcessing;

  constructor(batchProcessor, { batchSize, batchFrequency } = {}) {
    this.batchProcessor = batchProcessor;
    this.batchSize = batchSize || 10;
    this.batchFrequency = batchFrequency || 1000;

    this.#jobQueue = []; // fifo
    this.#timer = null;
    this.#isProcessing = false;
  }

  submitJob(job) {
    return new Promise((resolve, reject) => {
      this.#jobQueue.push({ job, resolve, reject });
      this.#startProcessing();
    });
  }

  get isProcessing() {
    return this.#isProcessing;
  }

  get jobQueue() {
    return this.#jobQueue;
  }

  /**
   * gate logic
   */
  #startProcessing() {
    if (!this.#isProcessing && this.#jobQueue.length >= this.batchSize) {
      this.#processBatch();
    } else if (!this.#timer) {
      this.#timer = setTimeout(() => {
        this.#processBatch();
      }, this.batchFrequency);
    }
  }

  #processBatch() {
    clearTimeout(this.#timer);
    this.#timer = null;
    this.#isProcessing = true;
    const jobsToProcess = this.#jobQueue.splice(0, this.batchSize);
    this.batchProcessor(jobsToProcess.map(({ job }) => job))
      .then((results) => {
        jobsToProcess.forEach(({ resolve }, index) => resolve(results[index]));
      })
      .catch((error) => {
        jobsToProcess.forEach(({ reject }) => reject(error));
      })
      .finally(() => {
        this.#isProcessing = false;
        if (this.#jobQueue.length) this.#processBatch();
      });
  }

  shutdown() {
    return new Promise((resolve) => {
      if (!this.#isProcessing) {
        resolve();
        clearTimeout(this.#timer);
        this.#timer = null;
        this.#jobQueue = [];
      } else {
        const intervalId = setInterval(() => {
          if (!this.#isProcessing) {
            clearInterval(intervalId);
            clearTimeout(this.#timer);
            this.#timer = null;
            this.#jobQueue = [];
            resolve();
          }
        }, 10);
      }
    });
  }
}

module.exports = MicroBatcher;
