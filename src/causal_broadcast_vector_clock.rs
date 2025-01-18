// In Vector clock, everything such as send and receive counts as an event.
// In order to implement causal delivery however, we should change the way : WE ONLY TRACT MESSAGE SENDs

use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        RwLock,
    },
    time::sleep,
};

async fn node(
    order_in_cluster: usize,
    outbound_channels: Vec<Sender<Vec<i32>>>,
    mut recv: Receiver<Vec<i32>>,
) {
    let vc = Arc::new(RwLock::new(vec![0, 0, 0]));

    // receiving process

    tokio::spawn({
        let vc_for_recv = vc.clone();
        async move {
            while let Some(msg) = recv.recv().await {
                println!("Node {} received vector clock: {:?}", order_in_cluster, msg);
                let mut vc = vc_for_recv.write().await;
                match take_pointwise_max(order_in_cluster, vc.clone(), msg.clone()) {
                    Ok(max) => {
                        *vc = max;
                    }
                    Err(_) => {
                        println!("--------------Concurrent events detected---------------");
                        println!("self vc: {:?}", vc);
                        println!("received vc: {:?}", msg);
                        println!("-------------------------------------------------------");
                    }
                };
            }
        }
    });

    // sending process
    tokio::spawn({
        let vc_for_send = vc.clone();

        async move {
            loop {
                // random sleep
                let mut vc = vc_for_send.write().await;
                // take max, try to find concurrent events

                vc[order_in_cluster] += 1;

                // send the message to other nodes except for self
                for i in outbound_channels.iter() {
                    let vc = vc.clone();
                    i.send(vc).await.unwrap();
                }
                sleep(Duration::from_secs(rand::random::<u64>() % 20)).await;
            }
        }
    });
}

fn take_pointwise_max(
    order_in_cluster: usize,
    self_vc: Vec<i32>,
    other_vc: Vec<i32>,
) -> Result<Vec<i32>, &'static str> {
    for i in self_vc.iter().zip(other_vc.iter()).enumerate() {
        let (index, (self_elem, other_elem)) = i;
        if index == order_in_cluster {
            continue;
        }
        if *self_elem > *other_elem {
            return Err("Concurrent events detected");
        }
    }
    return Ok(other_vc);
}

#[tokio::test]
pub async fn vector_clock_test() {
    let (tx1, rx1) = tokio::sync::mpsc::channel(10);
    let (tx2, rx2) = tokio::sync::mpsc::channel(10);
    let (tx3, rx3) = tokio::sync::mpsc::channel(10);

    tokio::spawn({
        let tx2_clone = tx2.clone();
        let tx3_clone = tx3.clone();
        node(0, vec![tx2_clone, tx3_clone], rx1)
    });

    tokio::spawn({
        let tx1_clone = tx1.clone();
        let tx3_clone = tx3.clone();
        node(1, vec![tx1_clone, tx3_clone], rx2)
    });

    tokio::spawn({
        let tx1_clone = tx1.clone();
        let tx2_clone = tx2.clone();
        node(2, vec![tx1_clone, tx2_clone], rx3)
    });

    tokio::time::sleep(std::time::Duration::from_secs(100)).await;
}
