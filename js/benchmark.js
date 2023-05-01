const Benchmark = require("benchmark");
const MicroBatcher = require("./microbatcher");

const suite = new Benchmark.Suite();

const batchProcessor = async (jobs) => {
  return Promise.all(jobs);
};

const runJobs = async (batchSize, batchFrequency) => {
  const microBatcher = new MicroBatcher(batchProcessor, {
    batchSize,
    batchFrequency,
  });
  const jobs = [];
  for (let i = 0; i < 1000; i++) {
    jobs.push(microBatcher.submitJob(Promise.resolve(`finished${i}`)));
  }
  return Promise.all(jobs);
};

suite
  .add("near real time", async function () {
    return runJobs(1, 0);
  })
  .add("a fast job", async function () {
    return runJobs(50, 10);
  })
  .add("a regular paced job", async function () {
    return runJobs(2000, 1000);
  })
  .add("a highly throttled job", async function () {
    return runJobs(2000, 5000);
  })
  .on("cycle", function (event) {
    console.log(String(event.target));
    console.log("time:", event.target.times.elapsed);
  })
  .run({ async: true });
