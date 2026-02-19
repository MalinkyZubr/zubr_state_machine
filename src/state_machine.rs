use std::sync::Arc;
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender}, watch::{channel as BrChannel, Receiver as BrReceiver, Sender as BrSender},
    Notify,
    RwLock,
};

/// Handle for reading outputs from a state machine and controlling its shutdown.
pub struct StateMachineOutputHandle<O> {
    output: Arc<RwLock<Option<O>>>,
    output_broadcast_receiver: BrReceiver<Option<O>>,
    shutdown_flag: Arc<Notify>,
}

impl<O: Clone> StateMachineOutputHandle<O> {
    /// Creates a new output handle with shared references to the state machine's output.
    fn new(
        output: Arc<RwLock<Option<O>>>,
        shutdown_flag: Arc<Notify>,
        output_broadcast_receiver: BrReceiver<Option<O>>,
    ) -> Self {
        Self {
            output,
            shutdown_flag,
            output_broadcast_receiver,
        }
    }

    /// Attempts to read the current output without blocking.
    /// Returns `None` if the lock is held or there's no output.
    pub fn try_read(&self) -> Option<O> {
        match self.output.try_read() {
            Ok(guard) => guard.clone(),
            Err(_) => None,
        }
    }

    /// Asynchronously reads the current output, waiting for the lock if necessary.
    pub async fn async_read(&self) -> Option<O> {
        self.output.read().await.clone()
    }

    /// Waits for the next state change and returns the new output.
    /// Returns `None` if the state machine has been shut down.
    pub async fn await_state_change(&mut self) -> Option<O> {
        match self.output_broadcast_receiver.changed().await {
            Ok(_) => self.output_broadcast_receiver.borrow().clone(),
            Err(_err) => None,
        }
    }

    /// Signals the state machine to shut down gracefully.
    pub fn close(&self) {
        self.shutdown_flag.notify_waiters();
    }
}

/// Handle for sending inputs to a state machine.
pub struct StateMachineInputHandle<I> {
    input_sender: Sender<I>,
}

impl<I: Clone> StateMachineInputHandle<I> {
    /// Creates a new input handle with a channel sender.
    fn new(input_sender: Sender<I>) -> Self {
        Self { input_sender }
    }

    /// Asynchronously sends an input to the state machine.
    /// Returns an error if the receiver has been dropped.
    pub async fn send_async(&self, input: I) -> Result<(), SendError<I>> {
        self.input_sender.send(input).await
    }

    /// Attempts to send an input without blocking.
    /// Returns an error if the channel is full or the receiver has been dropped.
    pub fn send(&self, input: I) -> Result<(), TrySendError<I>> {
        self.input_sender.try_send(input)
    }
}

/// A generic state machine that processes inputs and generates outputs based on state transitions.
pub struct StateMachine<I: Clone, T: Clone, O: Clone> {
    state: T,
    input_receiver: Receiver<I>,
    input_sender_template: Sender<I>,
    next_state_logic: fn(I, &T) -> T,
    output_logic: fn(&T) -> O,
    output: Arc<RwLock<Option<O>>>,
    output_broadcast_sender: BrSender<Option<O>>,
    shutdown_flag: Arc<Notify>,
}

impl<I: Clone, T: Clone, O: Clone> StateMachine<I, T, O> {
    /// Creates a new state machine with the given initial state and logic functions.
    ///
    /// # Arguments
    /// * `initial_state` - The starting state of the machine
    /// * `input_buffer_size` - Maximum number of pending inputs in the channel
    /// * `next_state_logic` - Function that computes the next state from input and current state
    /// * `output_logic` - Function that computes output from the current state
    pub fn new(
        initial_state: T,
        input_buffer_size: usize,
        next_state_logic: fn(I, &T) -> T,
        output_logic: fn(&T) -> O,
    ) -> Self {
        let (input_sender, input_receiver) = channel(input_buffer_size);
        let initial_output = output_logic(&initial_state);
        let output = Arc::new(RwLock::new(Some(initial_output.clone())));
        let (output_broadcast_sender, _) = BrChannel(Some(initial_output));
        Self {
            state: initial_state,
            input_receiver,
            input_sender_template: input_sender,
            next_state_logic,
            output_logic,
            output,
            output_broadcast_sender,
            shutdown_flag: Arc::new(Notify::new()),
        }
    }

    /// Processes a single input through the state machine's logic.
    /// Updates the state and broadcasts the new output to all listeners.
    async fn main_loop(&mut self, input: Option<I>) {
        match input {
            Some(input) => {
                let ns_logic_out = (self.next_state_logic)(input, &self.state);
                self.state = ns_logic_out;

                let output = Some((self.output_logic)(&self.state));
                let mut guard = self.output.write().await;
                *guard = output.clone();

                match self.output_broadcast_sender.send(output) {
                    Ok(_) => (),
                    Err(_err) => (),
                }
            }
            None => (),
        }
    }

    /// Runs the state machine's main event loop.
    /// Processes inputs until a shutdown signal is received.
    pub async fn run(&mut self) {
        let mut running = true;
        while running {
            tokio::select! {
                _ = self.shutdown_flag.notified() => {
                    running = false;
                    self.input_receiver.close();
                    *self.output.write().await = None;
                }
                input = self.input_receiver.recv() => {self.main_loop(input).await;}
            }
        }
    }

    /// Returns a reference to the current state.
    pub fn get_state(&self) -> &T {
        &self.state
    }

    /// Creates a new input handle for sending inputs to this state machine.
    pub fn spawn_input_handle(&self) -> StateMachineInputHandle<I> {
        StateMachineInputHandle::new(self.input_sender_template.clone())
    }

    /// Creates a new output handle for reading outputs from this state machine.
    pub fn spawn_output_handle(&self) -> StateMachineOutputHandle<O> {
        StateMachineOutputHandle::new(
            self.output.clone(),
            self.shutdown_flag.clone(),
            self.output_broadcast_sender.subscribe(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_1_input_1_output() {
        // Input: 5, Initial State: 0, Final State: 5 -> Output: 25
        let (mut sm, output_handle, input_handle) = create_state_machine(
            0u64,                         // Initial state
            |input, state| input + state, // Next state logic: sum of input and state
            |state| state * state,        // Output logic: square of state
            10,                           // Buffer size
        );

        // Run state machine in a separate task
        let join = tokio::spawn(async move {
            sm.run().await;
        });

        // Push a single input
        input_handle.send_async(5).await.unwrap();

        // Give state machine time to process
        sleep(Duration::from_millis(50)).await;

        // Read output
        assert_eq!(output_handle.try_read(), Some(25));

        // Push a single input
        input_handle.send_async(5).await.unwrap();

        // Give state machine time to process
        sleep(Duration::from_millis(50)).await;

        // Read output
        assert_eq!(output_handle.try_read(), Some(100));

        // Close output handle
        output_handle.close();
        let _ = join.await;
    }

    #[tokio::test]
    async fn test_many_input_1_output() {
        // Inputs: [1, 2, 3], Initial State: 0 -> Final State: 6 -> Output: 36
        let (mut sm, output_handle, input_handle) = create_state_machine(
            0u64,
            |input, state| input + state,
            |state| state * state,
            10,
        );

        let join = tokio::spawn(async move {
            sm.run().await;
        });

        // Push multiple inputs
        input_handle.send_async(1).await.unwrap();
        input_handle.send_async(2).await.unwrap();
        input_handle.send_async(3).await.unwrap();

        // Give state machine time to process
        sleep(Duration::from_millis(50)).await;

        // Read output
        assert_eq!(output_handle.try_read(), Some(36));

        // Close output handle
        output_handle.close();
        let _ = join.await;
    }

    #[tokio::test]
    async fn test_1_input_many_output() {
        // Input: 5, Initial State: 0 -> Final State: 5 -> Output: 25 for all listeners
        let (mut sm, output_handle, input_handle) = create_state_machine(
            0u64,
            |input, state| input + state,
            |state| state * state,
            10,
        );

        let output_handle_2 = sm.spawn_output_handle();

        let join = tokio::spawn(async move {
            sm.run().await;
        });

        input_handle.send_async(5).await.unwrap();

        sleep(Duration::from_millis(50)).await;

        assert_eq!(output_handle.try_read(), Some(25));
        assert_eq!(output_handle_2.try_read(), Some(25));

        output_handle.close();
        output_handle_2.close();
        let _ = join.await;
    }

    #[tokio::test]
    async fn test_many_input_many_output() {
        // Inputs: [2, 3], Initial State: 1 -> Final State: 6 -> Output: 36 for all listeners
        let (mut sm, output_handle, input_handle) = create_state_machine(
            1u64,
            |input, state| input + state,
            |state| state * state,
            10,
        );

        let output_handle_2 = sm.spawn_output_handle();

        let join = tokio::spawn(async move {
            sm.run().await;
        });

        input_handle.send_async(2).await.unwrap();
        input_handle.send_async(3).await.unwrap();

        sleep(Duration::from_millis(50)).await;

        assert_eq!(output_handle.try_read(), Some(36));
        assert_eq!(output_handle_2.try_read(), Some(36));

        output_handle.close();
        output_handle_2.close();

        let _ = join.await;
    }

    #[tokio::test]
    async fn test_output_handle_close() {
        let (mut sm, output_handle, input_handle) = create_state_machine(
            0u64,
            |input, state| input + state,
            |state| state * state,
            10,
        );

        let output_handle_2 = sm.spawn_output_handle();

        let join = tokio::spawn(async move {
            sm.run().await;
        });

        input_handle.send_async(1).await.unwrap();
        sleep(Duration::from_millis(50)).await;

        // Close one output handle
        output_handle.close();
        let _ = join.await;

        assert!(input_handle.send_async(2).await.is_err());
        sleep(Duration::from_millis(50)).await;

        assert_eq!(output_handle_2.try_read(), None);

        output_handle_2.close();
    }

    #[tokio::test]
    async fn test_input_channel_exhaustion() {
        let (mut sm, output_handle, input_handle) = create_state_machine(
            0u64,
            |input, state| input + state,
            |state| state * state,
            2, // Small buffer size to test exhaustion
        );

        let join = tokio::spawn(async move {
            sm.run().await;
        });

        // Fill the input buffer
        input_handle.send_async(1).await.unwrap();
        input_handle.send_async(2).await.unwrap();

        // The third send operation should fail if the buffer size is exceeded
        assert!(matches!(input_handle.send(3), Err(TrySendError::Full(3))));

        // Read output after processing two inputs
        sleep(Duration::from_millis(50)).await;
        assert_eq!(output_handle.try_read(), Some(9));

        output_handle.close();
        let _ = join.await;
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        let (mut sm, output_handle, input_handle) = create_state_machine(
            0u64,
            |input, state| input + state,
            |state| state * state,
            10,
        );

        let join = tokio::spawn(async move {
            sm.run().await;
        });

        input_handle.send_async(1).await.unwrap();
        sleep(Duration::from_millis(50)).await;

        // Shutdown the state machine
        output_handle.close();
        let _ = join.await;

        assert!(input_handle.send(2).is_err());
        assert!(input_handle.send_async(2).await.is_err());
    }

    #[tokio::test]
    async fn test_large_volume_stress_test() {
        let (mut sm, output_handle, input_handle) = create_state_machine(
            0u64,
            |input, state| input + state,
            |state| state * state,
            100,
        );

        let join = tokio::spawn(async move {
            sm.run().await;
        });

        let total_inputs: u64 = 1_000;
        for _ in 1..=total_inputs {
            input_handle.send_async(1).await.unwrap(); // Send 1 in each iteration
        }

        // Allow processing time
        sleep(Duration::from_millis(100)).await;

        // Validate final state and output
        assert_eq!(output_handle.try_read(), Some(total_inputs * total_inputs));

        output_handle.close();
        let _ = join.await;
    }

    #[tokio::test]
    async fn test_output_handle_async_read() {
        let (mut sm, _output_handle, input_handle) = create_state_machine(
            0u64,
            |input, state| input + state,
            |state| state * state,
            10,
        );

        let mut output_handle = sm.spawn_output_handle();
        let close_handle = sm.spawn_output_handle();

        let join = tokio::spawn(async move {
            sm.run().await;
        });

        let sender_join = tokio::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            input_handle.send_async(1).await.unwrap();
        });
        let result = output_handle.await_state_change().await;
        assert_eq!(result, Some(1));

        close_handle.close();
        let _ = sender_join.await;
        let _ = join.await;
    }

    /// Helper function to create a state machine with input and output handles for testing.
    fn create_state_machine<
        I: Clone + Send + 'static,
        T: Clone + Send + 'static,
        O: Clone + Send + 'static,
    >(
        initial_state: T,
        next_state_logic: fn(I, &T) -> T,
        output_logic: fn(&T) -> O,
        buffer_size: usize,
    ) -> (
        StateMachine<I, T, O>,
        StateMachineOutputHandle<O>,
        StateMachineInputHandle<I>,
    ) {
        let sm = StateMachine::new(initial_state, buffer_size, next_state_logic, output_logic);
        let output_handle = sm.spawn_output_handle();
        let input_handle = sm.spawn_input_handle();
        (sm, output_handle, input_handle)
    }
}
