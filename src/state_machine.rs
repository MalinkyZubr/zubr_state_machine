use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio::sync::mpsc::error::{SendError, TrySendError};

#[derive(Clone)]
pub struct StateMachineOutputHandle<O> {
    output: Arc<RwLock<O>>,
    shutdown_flag: Arc<AtomicBool>,
}
impl<O: Clone> StateMachineOutputHandle<O> {
    pub fn new(output: Arc<RwLock<O>>, shutdown_flag: Arc<AtomicBool>) -> Self {
        Self {
            output,
            shutdown_flag,
        }
    }
    pub fn read(&self) -> Option<O> {
        match self.output.read() {
            Ok(output) => Some(output.clone()),
            Err(_) => None,
        }
    }
    pub fn close(&self) {
        self.shutdown_flag
            .store(true, std::sync::atomic::Ordering::Release);
    }
}


#[derive(Clone)]
pub struct StateMachineInputHandle<I> {
    input_sender: Sender<I>,
}
impl<I: Clone> StateMachineInputHandle<I> {
    fn new(input_sender: Sender<I>) -> Self {
        Self { input_sender }
    }
    pub async fn send_async(&self, input: I) -> Result<(), SendError<I>>{
        self.input_sender.send(input).await
    }
    
    pub fn send(&self, input: I) ->  Result<(), TrySendError<I>> {
        self.input_sender.try_send(input)
    }
}

pub struct StateMachine<I: Clone, T: Clone, O: Clone> {
    state: T,
    input_receiver: Receiver<I>,
    next_state_logic: fn(I, &T) -> T,
    output_logic: fn(&T) -> O,
    output: Arc<RwLock<O>>,
    shutdown_flag: Arc<AtomicBool>,
}
impl<I: Clone, T: Clone, O: Clone> StateMachine<I, T, O> {
    fn new(
        state: T,
        input_receiver: Receiver<I>,
        next_state_logic: fn(I, &T) -> T,
        output_logic: fn(&T) -> O,
        output: Arc<RwLock<O>>,
        shutdown_flag: Arc<AtomicBool>,
    ) -> Self {
        Self {
            state,
            input_receiver,
            next_state_logic,
            output_logic,
            output,
            shutdown_flag,
        }
    }

    pub async fn run(&mut self) {
        while !self
            .shutdown_flag
            .load(std::sync::atomic::Ordering::Acquire)
        {
            match self.input_receiver.recv().await {
                Some(input) => {
                    let ns_logic_out = (self.next_state_logic)(input, &self.state);
                    self.state = ns_logic_out;
                    *self.output.write().unwrap() = (self.output_logic)(&self.state);
                }
                None => break,
            }
        }
        self.input_receiver.close();
        // flush the channel
        while self.input_receiver.len() > 0 {
            self.input_receiver.recv().await;
        }
    }
}


pub fn create_state_machine<I: Clone, T: Clone, O: Clone>(
    initial_state: T,
    next_state_logic: fn(I, &T) -> T,
    output_logic: fn(&T) -> O,
    buffsize: usize
) -> (StateMachine<I, T, O>, StateMachineOutputHandle<O>, StateMachineInputHandle<I>) {
    let input_channel = channel(buffsize);
    let output_guard = Arc::new(RwLock::new(output_logic(&initial_state)));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let state_machine = StateMachine::new(
        initial_state,
        input_channel.1,
        next_state_logic,
        output_logic,
        output_guard.clone(),
        stop_flag.clone(),
    );
    let output_handle = StateMachineOutputHandle::new(output_guard.clone(), stop_flag);
    let input_handle = StateMachineInputHandle::new(input_channel.0);

    (state_machine, output_handle, input_handle)
}
