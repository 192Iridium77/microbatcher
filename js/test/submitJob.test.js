const assert = require("assert");
const MicroBatcher = require("../microbatcher");

describe("Microbatcher", function () {
  let microBatcher;
  let batches;

  /**
   * recives a batch of jobs either when the limit is reached or at the end of a cycle
   * it then runs the jobs in parallel and resolves them
   * here we add the result batches so that we can observe the result across multiple batches
   */
  const batchProcessor = async (jobs) => {
    const result = await Promise.all(jobs);
    batches.push(result);
    return result;
  };

  beforeEach(() => {
    batches = [];
    microBatcher = new MicroBatcher(batchProcessor, {
      batchSize: 2,
      batchFrequency: 20,
    });
  });

  afterEach(async () => {
    return microBatcher.shutdown();
  });

  describe("submitJob", function () {
    it("should resolve a successful job", async function () {
      await microBatcher.submitJob(Promise.resolve("finished"));
      assert.deepEqual(batches, [["finished"]]);
    });

    it("should reject a failing job that doesn't hit the batch limit", async function () {
      try {
        await microBatcher.submitJob(Promise.reject("rejected"));
      } catch (error) {
        assert.equal(error, "rejected");
      }
    });

    it("should resolve two successful jobs in parallel", async function () {
      await Promise.all([
        microBatcher.submitJob(Promise.resolve("finished1")),
        microBatcher.submitJob(Promise.resolve("finished2")),
      ]);

      assert.deepEqual(batches, [["finished1", "finished2"]]);
    });

    it("should eventually resolve jobs that take longer than the batch frequency", async function () {
      const job1 = new Promise((resolve) => {
        setTimeout(() => {
          resolve("finished1");
        }, 10);
      });
      const job2 = Promise.resolve("finished2");

      await Promise.all([
        microBatcher.submitJob(job1),
        microBatcher.submitJob(job2),
      ]);

      assert.deepEqual(batches, [["finished1", "finished2"]]);
    });

    it("should resolve jobs in batches", function () {
      return Promise.all([
        microBatcher.submitJob(Promise.resolve("finished1")),
        microBatcher.submitJob(Promise.resolve("finished2")),
        microBatcher.submitJob(Promise.resolve("finished3")),
        microBatcher.submitJob(Promise.resolve("finished4")),
      ]).then(() => {
        assert.deepEqual(batches, [
          ["finished1", "finished2"],
          ["finished3", "finished4"],
        ]);
      });
    });
  });
});
