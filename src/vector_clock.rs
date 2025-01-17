use rand::{thread_rng, Rng};
use std::sync::Arc;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    RwLock,
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
                match take_pointwise_max(vc.clone(), msg.clone()) {
                    Ok(max) => {
                        *vc = max;
                        vc[order_in_cluster] += 1;
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
                // choose a random node to send the message to
                let random_node = thread_rng().gen_range(0..outbound_channels.len());
                // random sleep
                tokio::time::sleep(std::time::Duration::from_secs(rand::random::<u64>() % 20))
                    .await;
                let mut vc = vc_for_send.write().await;
                // take max, try to find concurrent events

                vc[order_in_cluster] += 1;

                outbound_channels[random_node]
                    .send(vc.clone())
                    .await
                    .unwrap();
            }
        }
    });
}

fn take_pointwise_max(self_vc: Vec<i32>, other_vc: Vec<i32>) -> Result<Vec<i32>, &'static str> {
    // every elements in other_vc has to be bigger than or equal to self's

    self_vc
        .into_iter()
        .zip(other_vc.into_iter())
        .map(|(a, b)| if a > b { Err(()) } else { Ok(b) })
        .collect::<Result<Vec<_>, ()>>()
        .map_err(|_| "Concurrent events detected")
}

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
