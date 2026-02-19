# zubr_state_machine

![Waltuh. State Machine](https://github.com/MalinkyZubr/zubr_state_machine/blob/main/statemachine.png)
[Template taken from here](https://github.com/zaszi/rust-template/blob/master/README.md)

A simple asynchronous state machine library for creating Mealy machines in rust.

## Features

- Imitation of hardware state machine design in HDL
    - each state machine requires next state and output logic
- Multiple concurrent inputs supported
- Multiple concurrent outputs supported with both synchronous and asynchronous output read
- built using tokio

## Download

Github page [here](https://github.com/MalinkyZubr/ZubrStateMachine)

## Usage

- every state machine consists of 3 main parts. Input, State, and Output
- next state logic and output logic determine how these 3 interact
    - **next state logic** returns the desired next state of the state machine based on input and current state
        - it executes whenever an input is detected
    - **output logic** returns the desired output for the state determined by next state logic
- you can create a state machine as shown below

```aiignore
let mut sm = StateMachine::<InputType, StateType, OutputType>::new(
    initial_state: StateType, 
    input_buffer_size: usize, 
    next_state_logic: fn(InputType, &StateType) -> T, 
    output_logic: fn(&StateType) -> O
);  
```

- after state machine instantiation you can spawn it's input and output handles
- **input handles** allow a producer to send data either synchronously (no await) or asynchronously (await)
    - the caller is responsible for error handling. This is a thinly veiled wrapper around tokio::mpsc::Sender
- **output handles** allow a consumer to read the output of the state machine.
    - Output is read one of two ways
        - *on demand*: the handle awaits on a read() acquire on a tokio RwLock to get a copy of state machine output
        - *on notification*: the handle awaits a message from a tokio watch, and returns it when the output updates
    - The output handle controls the state machine lifecycle. Any call to close() will shut the state machine down if it
      is running as a task.
- how to spawn these handles is shown

```aiignore
let mut output_handle = sm.spawn_output_handle();
let mut input_handle = sm.spawn_input_handle();
```

- A simple SM operation is shown below
```aiignore
use std::time::Duration;
use tokio::time::sleep;
use ZubrStateMachine::*;



#[tokio::main]
async fn main() {
    let mut sm = StateMachine::<u64, u64, u64>::new(
        0u64, // initial state set to 0 
        10, // maximum input buffer size
        |input, state| input + state, // calculate state by adding the input and current state 
        |state| state * state, // output is the square of the current state
    );
    let mut output_handle = sm.spawn_output_handle();
    let input_handle_async = sm.spawn_input_handle();
    let input_handle_sync = sm.spawn_input_handle();

    let join = tokio::spawn(async move { // the state machine will be long lived and loop until .close() is called from an output handle
        sm.run().await;
    });

    let async_input_join = tokio::spawn(async move { // spawns an asynchronous task to send a piece of data
        sleep(Duration::from_millis(500)).await;
        input_handle_async.send_async(1).await.unwrap();
    });

    let asynchronous_result = output_handle.await_state_change().await.unwrap(); // wait for an update to be pushed to the output value, read that new value
    let _ = input_handle_sync.send(2);
    sleep(Duration::from_millis(50)).await;
    let synchronous_result = output_handle.try_read().unwrap(); // await RwLock acquisition and get whatever value is inside

    println!("{}", asynchronous_result); // should print 1
    println!("{}", synchronous_result); // should print 9
    
    output_handle.close();
    let _ = join.await;
    let _ = async_input_join.await;
}
```

## Contribution

I dont anticipate many people looking at this, and its a tiny library. But please do as you please to improve it! Open
issue, pull request, I will do my best to engage.

## License

MIT license