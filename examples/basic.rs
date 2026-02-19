use std::time::Duration;
use tokio::time::sleep;
use zubr_state_machine::*;



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
