// Copyright 2018-2019 Parity Technologies (UK) Ltd.
// This file is part of substrate-archive.

// substrate-archive is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// substrate-archive is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with substrate-archive. If not, see <http://www.gnu.org/licenses/>.
#[allow(warnings, unused)]

mod registry;
mod job;
mod types;
mod error;
mod db;
mod threadpool;

use std::{
    any::Any,
    panic::{catch_unwind, AssertUnwindSafe, PanicInfo, RefUnwindSafe, UnwindSafe},
    sync::Arc,
    time::Duration,
};

use lapin::{Connection, ConnectionProperties, Channel, Queue, ExchangeKind};
use async_amqp::*;
use channel::Sender;
use async_std::task;

use crate::{
    job::Job,
    error::*,
    registry::Registry,
};

const TASK_QUEUE: &str = "TASK_QUEUE";

/// Builder pattern struct for the Runner
pub struct Builder<Env> {
    environment: Env,
    num_threads: Option<usize>,
    addr: String,
    registry: Registry<Env>,
    /// Amount of time to wait until job is deemed a failure
    timeout: Option<Duration>,
}

impl<Env: 'static> Builder<Env> {
    /// Instantiate a new instance of the Builder
    pub fn new<S: Into<String>>(environment: Env, addr: S) -> Self {
        let addr: String = addr.into();
        Self {
            environment,
            addr,
            num_threads: None,
            registry: Registry::load(),
            timeout: None,
        }
    }

    ///  Register a job that hasn't or can't be registered by invoking the `register_job!` macro
    ///
    /// Jobs that include generics must use this function in order to be registered with a runner.
    /// Jobs must be registered with every generic that is used.
    /// Jobs are available in the format `my_function_name::Job`.
    ///
    ///  # Example
    ///  ```ignore
    ///  RunnerBuilder::new(env, conn)
    ///      .register_job::<resize_image::Job<String>>()
    ///  ```
    ///  Different jobs must be registered with different generics if they exist.
    ///
    ///  ```ignore
    ///  RunnerBuilder::new((), conn)
    ///     .register_job::<resize_image::Job<String>>()
    ///     .register_job::<resize_image::Job<u32>>()
    ///     .register_job::<resize_image::Job<MyStruct>()
    ///  ```
    ///
    pub fn register_job<T: Job + 'static + Send>(mut self) -> Self {
        self.registry.register_job::<T>();
        self
    }

    /// specify the amount of threads to run the threadpool with
    pub fn num_threads(mut self, threads: usize) -> Self {
        self.num_threads = Some(threads);
        self
    }

    /// Set a timeout in seconds.
    /// This timeout is the maximum amount of time coil will wait for a job to begin
    /// before returning an error.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Build the runner
    pub fn build(self) -> Result<Runner<Env>, Error> {
        let threadpool = ThreadPool::with_name(
            "coil-worker".to_string(),
            self.num_threads.unwrap_or(num_cpus::get()),
        );

        let timeout = self
            .timeout
            .unwrap_or_else(|| std::time::Duration::from_secs(5));
        
        let conn = Connection::connect(
            &self.addr,
            ConnectionProperties::default().with_async_std(),
        ).wait()?;

        Ok(Runner {
            threadpool,
            conn, 
            environment: Arc::new(self.environment),
            registry: Arc::new(self.registry),
            timeout,
        })
    }
}

/// Runner for background tasks.
/// Synchronous tasks are run in a threadpool.
pub struct Runner<Env> {
    threadpool: ThreadPool,
    conn: lapin::Connection, 
    // queue: QueueHandle,
    environment: Arc<Env>,
    registry: Arc<Registry<Env>>,
    timeout: Duration,
}

#[derive(Debug)]
pub enum Event {
    /// Queues are currently working
    Working,
    /// No more jobs available in queue
    NoJobAvailable,
    /// An error occurred loading the job from the database
    ErrorLoadingJob(lapin::Error),
}

struct QueueHandle {
    channel: Channel,
    queue: Queue
}

impl QueueHandle {
    fn new(connection: Connection) -> Self {
        let channel = connection.create_channel().wait().unwrap();
        let queue = channel.queue_declare(
            &TASK_QUEUE,
            Default::default(),
            Default::default()
        ).wait().unwrap();
        
        Self { channel, queue }
    }
    
    /// Push to the RabbitMQ
    fn push(&self, payload: Vec<u8>) -> Result<(), lapin::Error> {
        self.channel.exchange_declare("", ExchangeKind::Direct, Default::default(), Default::default()).wait()?;
        self.channel.basic_publish("", TASK_QUEUE, Default::default(), payload, Default::default()).wait()?;
        Ok(())
    }

    fn pop(&self) -> Result<Vec<u8>, lapin::Error> {
        todo!();    
    }
}

// Methods which don't require `RefUnwindSafe`
impl<Env: 'static> Runner<Env> {
    /// Build the builder for `Runner`
    pub fn builder(env: Env, conn: &str) -> Builder<Env> {
        Builder::new(env, conn)
    }

    /// Get a Pool Connection from the pool that the runner is using.
    pub async fn connection(&self) -> &lapin::Connection {
        &self.conn
    }
}


/// NOTE: need to consume here
impl<Env: Send + Sync + RefUnwindSafe + 'static> Runner<Env> {
    /// Runs all the pending tasks in a loop
    /// Returns how many tasks are running as a result
    pub fn run_pending_tasks(&self) -> Result<(), FetchError> {
        let max_threads = self.threadpool.max_count();
        let (tx, rx) = channel::bounded(max_threads);
        tracing::trace!("Max Threads: {}", max_threads);

        let mut pending_messages = 0;
        loop {
            let available_threads = max_threads - self.threadpool.active_count();

            let jobs_to_queue = if pending_messages == 0 {
                std::cmp::max(available_threads, 1)
            } else {
                available_threads
            };

            for _ in 0..jobs_to_queue {
                self.run_single_sync_job(tx.clone())
            }

            pending_messages += jobs_to_queue;
            match rx.recv_timeout(self.timeout) {
                Ok(Event::Working) => pending_messages -= 1,
                Ok(Event::NoJobAvailable) => return Ok(()),
                Ok(Event::ErrorLoadingJob(e)) => return Err(FetchError::FailedLoadingJob(e)),
                Err(channel::RecvTimeoutError::Timeout) => return Err(FetchError::Timeout.into()),
                Err(channel::RecvTimeoutError::Disconnected) => {
                    tracing::warn!("Job sender disconnected!");
                    return Err(FetchError::Timeout.into());
                }
            }
        }
    }
    
    fn run_single_sync_job(&self, tx: Sender<Event>) {
        let env = Arc::clone(&self.environment);
        let registry = Arc::clone(&self.registry);

        self.get_single_job(tx, move |job| {
            let perform_fn = registry
                .get(&job.job_type)
                .ok_or_else(|| PerformError::from(format!("Unknown job type {}", job.job_type)))?;
            perform_fn.perform(job.data, &env, &"Connection goes here".to_string())
        });
    }
    
    fn get_single_job<F>(&self, tx: Sender<Event>, fun: F)
    where
        F: FnOnce(db::BackgroundJob) -> Result<(), PerformError> + Send + UnwindSafe + 'static,
    {
        self.threadpool.execute(move || {
            match (|| {
                let job = Self::next_job(tx.clone()).ok_or(Error::Msg("Failed to get unlocked job".into()))?;
                catch_unwind(|| fun(job))
                    .map_err(|e| try_to_extract_panic_info(&e))
                    .and_then(|r| r)
            })() {
                Ok(_) => { 
                    tracing::trace!("Job Success");
                    // send ACK, allowing job to be dropped from queue
                },
                Err(e) => {
                    tracing::error!("Job failed to run {}", e);
                    eprintln!("Job failed to run: {}", e);
                    // ACK
                    // re-queue the JOB after incrementing `retries`
                    // db::requeue_failed_job();
                    // panic!("Failed to update job: {:?}", e);
                }
            }
        })
    }

/* 
    fn get_single_job<F>(&self, tx: Sender<Event>, fun: F)
    where
        F: FnOnce(db::BackgroundJob) -> Result<(), PerformError> + Send + UnwindSafe + 'static,
    {
        
        let pg_pool = self.pg_pool.clone();
        self.threadpool.execute(move || {
            let res = move || -> Result<(), PerformError> {
                let (mut transaction, job) =
                    if let Some((t, j)) = block_on(Self::get_next_job(tx, &pg_pool)) {
                        (t, j)
                    } else {
                        return Ok(());
                    };
                let job_id = job.id;
                let result = catch_unwind(|| fun(job))
                    .map_err(|e| try_to_extract_panic_info(&e))
                    .and_then(|r| r);
                match result {
                    Ok(_) => block_on(db::delete_successful_job(&mut transaction, job_id))?,
                    Err(e) => {
                        eprintln!("Job {} failed to run: {}", job_id, e);
                        block_on(db::update_failed_job(&mut transaction, job_id))?
                    }
                }
                block_on(transaction.commit())?;
                Ok(())
            };

            match res() {
                Ok(_) => {}
                Err(e) => {
                    panic!("Failed to update job: {:?}", e);
                }
            }
        });
    }
   */ 
    
    fn next_job(tx: Sender<Event>) -> Option<db::BackgroundJob> {
        todo!();     
    }

    /*
    /// returns a transaction/job pair for the next Job
    async fn get_next_job(tx: Sender<Event>, pg_pool: &PgPool) -> TxJobPair {
        puffin::profile_function!(); 
        let now = std::time::Instant::now();
        let mut transaction = match pg_pool.begin().await {
            Ok(t) => t,
            Err(e) => {
                let _ = tx.send(Event::ErrorLoadingJob(e));
                return None;
            }
        };
        if now.elapsed() > Duration::from_secs(1) {
            log::warn!("Took {:?} to start a transaction!!!", now.elapsed());
        }

        let job = match db::find_next_unlocked_job(&mut transaction).await {
            Ok(Some(j)) => {
                let _ = tx.send(Event::Working);
                j
            }
            Ok(None) => {
                let _ = tx.send(Event::NoJobAvailable);
                return None;
            }
            Err(e) => {
                let _ = tx.send(Event::ErrorLoadingJob(e));
                return None;
            }
        };
        if now.elapsed() > Duration::from_secs(1) {
            log::warn!(
                "Took {:?} to start a transaction and find a job",
                now.elapsed()
            );
        }
        Some((transaction, job))
    }
    */
}

fn try_to_extract_panic_info(info: &(dyn Any + Send + 'static)) -> PerformError {
    if let Some(x) = info.downcast_ref::<PanicInfo>() {
        format!("job panicked: {}", x).into()
    } else if let Some(x) = info.downcast_ref::<&'static str>() {
        format!("job panicked: {}", x).into()
    } else if let Some(x) = info.downcast_ref::<String>() {
        format!("job panicked: {}", x).into()
    } else {
        "job panicked".into()
    }
}

/*
#[cfg(any(test, feature = "test_components"))]
impl<Env: Send + Sync + RefUnwindSafe + 'static> Runner<Env> {
    /// Wait for tasks to finish based on timeout
    /// this is mostly used for internal tests
    fn wait_for_all_tasks(&self) -> Result<(), String> {
        self.threadpool.join();
        let panic_count = self.threadpool.panic_count();
        if panic_count == 0 {
            Ok(())
        } else {
            Err(format!("{} threads panicked", panic_count).into())
        }
    }

    /// Check for any jobs that may have failed
    pub async fn check_for_failed_jobs(&self) -> Result<(), FailedJobsError> {
        self.wait_for_all_tasks().unwrap();
        let num_failed = db::failed_job_count(&self.pg_pool).await.unwrap();
        if num_failed == 0 {
            Ok(())
        } else {
            Err(FailedJobsError::JobsFailed(num_failed))
        }
    }
}

*/

