// In Vector clock, everything such as send and receive counts as an event.
// In order to implement causal delivery however, we should change the way : WE ONLY TRACT MESSAGE SENDs
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use tokio::time::sleep;

fn node(
    order_in_cluster: usize,
    outbound_channels: Vec<Sender<Vec<i32>>>,
    mut recv: Receiver<Vec<i32>>,
) {
    let vc = Arc::new(RwLock::new(vec![0, 0, 0]));
    tokio::spawn({
        let vc_for_recv = vc.clone();
        async move {
            while let Some(msg) = recv.recv().await {
                println!("Node {} received {:?}", order_in_cluster + 1, msg);
                let mut vc = vc_for_recv.write().await;

                if let Err(err) = take_pointwise_max(order_in_cluster, vc.clone(), msg.clone()) {
                    println!("{}", err);
                    // TODO: Buffering logic
                } else {
                    *vc = msg;
                }
            }
        }
    });
    tokio::spawn({
        let vc_for_send = vc.clone();
        async move {
            loop {
                {
                    let mut vc = vc_for_send.write().await;
                    vc[order_in_cluster] += 1;
                }
                for channel in outbound_channels.iter() {
                    let vc = vc_for_send.read().await.clone();
                    if channel.send(vc).await.is_err() {
                        eprintln!("Failed to send vector clock");
                    }
                }
                let delay = rand::random::<u64>() % 4;
                sleep(Duration::from_secs(delay)).await;
            }
        }
    });
}

fn take_pointwise_max(
    order_in_cluster: usize,
    self_vc: Vec<i32>,
    other_vc: Vec<i32>,
) -> Result<Vec<i32>, String> {
    for (index, (&self_elem, &other_elem)) in self_vc.iter().zip(&other_vc).enumerate() {
        if index != order_in_cluster && self_elem > other_elem {
            return Err(format!(
                "--------------Concurrent events detected---------------\n\
Node {} detected concurrent events\n\
self vc: {self_vc:?}\n\
received vc: {other_vc:?}\n\
-------------------------------------------------------",
                order_in_cluster + 1
            ));
        }
    }
    Ok(other_vc)
}

#[tokio::test]
pub async fn vector_clock_test() {
    let (tx1, rx1) = tokio::sync::mpsc::channel(10);
    let (tx2, rx2) = tokio::sync::mpsc::channel(10);
    let (tx3, rx3) = tokio::sync::mpsc::channel(10);

    node(0, vec![tx2.clone(), tx3.clone()], rx1);
    node(1, vec![tx1.clone(), tx3.clone()], rx2);
    node(2, vec![tx1.clone(), tx2.clone()], rx3);

    sleep(Duration::from_secs(10)).await;
}
