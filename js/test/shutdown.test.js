const assert = require("assert");
const MicroBatcher = require("../microbatcher");

describe("Microbatcher", function () {
  const batches = [];

  const batchProcessor = (jobs) => {
    batches.push(jobs);
    return Promise.all(jobs);
  };

  describe("shutdown", function () {
    it("should shutdown straight away when there are no jobs running", function () {
      const microBatcher = new MicroBatcher(batchProcessor, {
        batchSize: 2,
        batchFrequency: 200,
      });
      microBatcher.shutdown().then(() => {
        assert(!microBatcher.isProcessing);
      });
    });
    it("should wait to finish processing jobs before shutting down", function (done) {
      const microBatcher = new MicroBatcher(batchProcessor, {
        batchFrequency: 20,
      });

      microBatcher.submitJob(
        new Promise((resolve) => {
          setTimeout(() => {
            console.log("resolving!");
            resolve("finished");
          }, 50);
        })
      );

      setTimeout(() => {
        assert.ok(microBatcher.isProcessing);

        const shutdownPromise = microBatcher.shutdown();

        shutdownPromise.then(() => {
          assert(!microBatcher.isProcessing);
          done();
        });
      }, 30);
    });
  });
});
