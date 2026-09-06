//! End to end against a real msgsrvd.
//!
//! Every assertion about what the log holds is made by restarting the
//! server and reading the record count off its recovery scan, which is
//! the only way to see inside the log until the read path exists.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;

use common::*;
use msgsrv_client::proto::{err, flag, op, Header};
use msgsrv_client::{Client, Durability, Error, Watch, WriteAck};

/* ---- tests ---- */

#[tokio::test]
async fn writes_are_acknowledged_after_the_flush() {
    let dir = tempfile::tempdir().unwrap();
    let port = test_port();
    let mut srv = Server::start(dir.path(), port).await;
    assert_eq!(srv.records, 0, "a fresh log holds nothing");

    let c = Client::connect(config(srv.addr())).await.unwrap();

    let mut last = 0;
    for i in 0..64u64 {
        let ack = c
            .write(i % 4, format!("record {i}").into_bytes(), Durability::Sync)
            .await
            .unwrap();
        let seq = ack
            .seq
            .expect("a live acknowledgement names a WAL sequence");
        assert!(seq > last, "WAL sequences advance: {seq} after {last}");
        assert!(!ack.degraded);
        last = seq;
    }

    srv.kill().await;
    assert_eq!(recovered_records(dir.path(), port).await, 64);
}

/// The point of the pipeline: many writes outstanding at once, one
/// acknowledgement covering a batch of them.
#[tokio::test]
async fn writes_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let port = test_port();
    let mut srv = Server::start(dir.path(), port).await;

    let c = Client::connect(config(srv.addr())).await.unwrap();

    let mut waiting = Vec::new();
    for i in 0..500u64 {
        let c = c.clone();
        waiting.push(tokio::spawn(async move {
            c.write(i % 8, format!("r{i}").into_bytes(), Durability::Sync)
                .await
        }));
    }

    let mut seqs = Vec::new();
    for w in waiting {
        seqs.push(w.await.unwrap().unwrap().seq.unwrap());
    }
    /* Cumulative acknowledgement: a batch resolves on one frame, so
     * many records share the sequence that covered them. */
    seqs.sort_unstable();
    seqs.dedup();
    assert!(
        seqs.len() < 500,
        "every write got its own flush, so nothing batched"
    );

    srv.kill().await;
    assert_eq!(recovered_records(dir.path(), port).await, 500);
}

#[tokio::test]
async fn replication_is_refused_rather_than_promised() {
    let dir = tempfile::tempdir().unwrap();
    let port = test_port();
    let mut srv = Server::start(dir.path(), port).await;

    let c = Client::connect(config(srv.addr())).await.unwrap();

    match c
        .write(1, b"needs two copies".to_vec(), Durability::Replicated)
        .await
    {
        Err(Error::NoReplicas) => {}
        other => panic!("expected NoReplicas, got {other:?}"),
    }

    /* The connection survives the refusal, and a caller that says it
     * will take one copy gets one, and is told so. */
    let ack = c
        .write(
            1,
            b"one copy will do".to_vec(),
            Durability::ReplicatedOrDegraded,
        )
        .await
        .unwrap();
    assert!(ack.degraded, "a leader-only copy has to say so");

    srv.kill().await;
    assert_eq!(
        recovered_records(dir.path(), port).await,
        1,
        "the refused record was not written"
    );
}

/// Kill the server under a client with writes outstanding: it
/// reconnects, resumes, resends, and every record lands once.
#[tokio::test]
async fn a_killed_server_costs_nothing_acknowledged() {
    let dir = tempfile::tempdir().unwrap();
    let port = test_port();
    let mut srv = Server::start(dir.path(), port).await;

    let c = Client::connect(config(srv.addr())).await.unwrap();
    let session = c.session();

    for i in 0..50u64 {
        c.write(0, format!("before {i}").into_bytes(), Durability::Sync)
            .await
            .unwrap();
    }

    srv.kill().await;

    /* Issued while there is no server to take them. They sit in the
     * queue until one exists. */
    let mut waiting = Vec::new();
    for i in 0..10u64 {
        let c = c.clone();
        waiting.push(tokio::spawn(async move {
            c.write(0, format!("after {i}").into_bytes(), Durability::Sync)
                .await
        }));
    }

    let mut srv = Server::start(dir.path(), port).await;
    assert_eq!(srv.records, 50, "everything acknowledged survived the kill");

    for w in waiting {
        w.await
            .unwrap()
            .expect("a queued write lands once the server is back");
    }
    assert_eq!(c.session(), session, "the session id does not change");

    srv.kill().await;
    assert_eq!(
        recovered_records(dir.path(), port).await,
        60,
        "resends were deduplicated"
    );
}

/// The server's dedup, driven directly: resend sequences it already
/// holds, across a restart, and see them acknowledged and not stored.
#[tokio::test]
async fn a_resend_is_acknowledged_not_stored() {
    let dir = tempfile::tempdir().unwrap();
    let port = test_port();
    let mut srv = Server::start(dir.path(), port).await;

    let mut sock = TcpStream::connect(srv.addr()).await.unwrap();
    let (session, mark) = hello(&mut sock, 0).await.unwrap();
    assert_eq!(mark, 0, "a new session has nothing durable");

    for seq in 1..=5u64 {
        let ack = raw_write(&mut sock, seq, b"body").await;
        assert_eq!(ack.op, op::ACK);
    }
    drop(sock);

    /* The session table is rebuilt from the log, so a restart must
     * not lose what the session had made durable. */
    srv.kill().await;
    let mut srv = Server::start(dir.path(), port).await;
    assert_eq!(srv.records, 5);

    let mut sock = TcpStream::connect(srv.addr()).await.unwrap();
    let (resumed, mark) = hello(&mut sock, session).await.unwrap();
    assert_eq!(resumed, session);
    assert_eq!(mark, 5, "the resume says where to carry on from");

    for seq in 3..=7u64 {
        let ack = raw_write(&mut sock, seq, b"body").await;
        assert_eq!(ack.op, op::ACK);
    }
    drop(sock);

    srv.kill().await;
    assert_eq!(
        recovered_records(dir.path(), port).await,
        7,
        "3 to 5 were already held, only 6 and 7 were new"
    );
}

#[tokio::test]
async fn a_forgotten_session_is_named_rather_than_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let port = test_port();
    let mut srv = Server::start(dir.path(), port).await;

    let mut sock = TcpStream::connect(srv.addr()).await.unwrap();
    match hello(&mut sock, 12345).await {
        Err(code) => assert_eq!(code, err::SESSION_UNKNOWN),
        Ok(_) => panic!("a session the server never issued was resumed"),
    }

    srv.kill().await;
}

#[tokio::test]
async fn a_record_over_the_cap_never_reaches_the_wire() {
    let dir = tempfile::tempdir().unwrap();
    let port = test_port();
    let mut srv = Server::start(dir.path(), port).await;

    let mut cfg = config(srv.addr());
    cfg.max_payload = 128;
    let c = Client::connect(cfg).await.unwrap();

    match c.write(0, vec![0u8; 129], Durability::Sync).await {
        Err(Error::PayloadTooBig { len, max }) => {
            assert_eq!((len, max), (129, 128));
        }
        other => panic!("expected PayloadTooBig, got {other:?}"),
    }

    /* Refused locally, so the connection is untouched. */
    c.write(0, vec![0u8; 128], Durability::Sync).await.unwrap();

    srv.kill().await;
    assert_eq!(recovered_records(dir.path(), port).await, 1);
}

/// A client that leaves before reading its acknowledgements must not
/// take the server with it.
///
/// A send to a socket whose peer has gone raises SIGPIPE unless the
/// send says otherwise, and the daemon blocks no signal it does not
/// read through a descriptor, so the default action would end the
/// process. It takes two sends to see it: the first is answered with a
/// reset, and only the one after that gets EPIPE. So the writes are
/// pipelined deep enough that the acknowledgements do not fit in one.
/// Every other test here reads its replies, which is why this one has
/// to exist.
#[tokio::test]
async fn a_client_that_leaves_before_its_reply_does_not_take_the_server() {
    let dir = tempfile::tempdir().unwrap();
    let port = test_port();
    let mut srv = Server::start(dir.path(), port).await;

    for round in 1..=8u64 {
        let mut sock = TcpStream::connect(srv.addr()).await.unwrap();
        hello(&mut sock, 0)
            .await
            .expect("the server is still taking connections");

        for i in 1..=300u64 {
            let h = Header::new(
                op::WRITE,
                flag::ACK_REQ | flag::SYNC,
                4,
                7,
                round * 1000 + i,
            );
            send_frame(&mut sock, &h, b"gone").await;
        }
        drop(sock);
    }

    /* Still up, and still taking writes. */
    let c = Client::connect(config(srv.addr())).await.unwrap();
    c.write(0, b"after".to_vec(), Durability::Sync)
        .await
        .expect("the server survived every client that walked away");

    srv.kill().await;
}

#[tokio::test]
async fn a_server_that_is_down_is_reported_at_connect() {
    let port = test_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    match Client::connect(config(addr)).await {
        Err(Error::Io(_)) => {}
        other => panic!("expected an io error, got {other:?}"),
    }
}

/* ---- replication ---- */

/// Write until the replica has attached, or give up.
///
/// A leader with no replica refuses a replicated write outright, and
/// the replica takes a moment to dial in, so the first writes here are
/// expected to fail. Every one of them is refused rather than taken on
/// a promise, which is the property being relied on.
async fn write_when_replicated(c: &Client, i: u64) -> WriteAck {
    for _ in 0..100 {
        match c
            .write(0, format!("r{i}").into_bytes(), Durability::Replicated)
            .await
        {
            Ok(ack) => return ack,
            Err(Error::NoReplicas) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("replicated write failed: {e:?}"),
        }
    }
    panic!("the replica never attached");
}

#[tokio::test]
async fn a_replicated_write_waits_for_the_replica() {
    let ldir = tempfile::tempdir().unwrap();
    let fdir = tempfile::tempdir().unwrap();
    let lport = test_port();
    let fport = test_port();

    let mut leader = Server::start(ldir.path(), lport).await;
    let mut follower = Server::start_replica(fdir.path(), fport, lport).await;

    let c = Client::connect(config(leader.addr())).await.unwrap();

    let ack = write_when_replicated(&c, 0).await;
    assert!(!ack.degraded, "a write with a replica up is not degraded");

    for i in 1..64u64 {
        let ack = c
            .write(i % 4, format!("r{i}").into_bytes(), Durability::Replicated)
            .await
            .unwrap();
        assert!(!ack.degraded);
    }

    /* The replica is only told about records the leader has flushed,
     * so what it holds is what an acknowledgement promised. */
    follower.kill().await;
    leader.kill().await;

    let held = recovered_records(ldir.path(), lport).await;
    assert_eq!(
        recovered_records(fdir.path(), fport).await,
        held,
        "the replica holds a different log from the leader"
    );
}

#[tokio::test]
async fn a_lost_replica_costs_the_replicated_level_and_nothing_else() {
    let ldir = tempfile::tempdir().unwrap();
    let fdir = tempfile::tempdir().unwrap();
    let lport = test_port();
    let fport = test_port();

    let mut leader = Server::start(ldir.path(), lport).await;
    let mut follower = Server::start_replica(fdir.path(), fport, lport).await;

    let c = Client::connect(config(leader.addr())).await.unwrap();
    write_when_replicated(&c, 0).await;

    follower.kill().await;

    match c
        .write(0, b"needs two".to_vec(), Durability::Replicated)
        .await
    {
        Err(Error::NoReplicas) => {}
        other => panic!("expected NoReplicas, got {other:?}"),
    }

    let ack = c
        .write(0, b"one will do".to_vec(), Durability::ReplicatedOrDegraded)
        .await
        .unwrap();
    assert!(ack.degraded, "a leader-only copy has to say so");

    /* A weaker level is unaffected: it never depended on the replica. */
    let ack = c
        .write(0, b"local".to_vec(), Durability::Sync)
        .await
        .unwrap();
    assert!(!ack.degraded);

    /*
     * A replica that comes back asks from the sequence it holds, and
     * the leader reads the rest out of its log. That is the only path
     * that streams records the leader is no longer holding in hand.
     */
    let follower = Server::start_replica(fdir.path(), fport, lport).await;
    write_when_replicated(&c, 1).await;

    let mut follower = follower;
    follower.kill().await;
    leader.kill().await;

    let held = recovered_records(ldir.path(), lport).await;
    assert_eq!(
        recovered_records(fdir.path(), fport).await,
        held,
        "the replica did not catch up on what it missed"
    );
}

/* ---- following the log ---- */

/// A subscriber is told about records as they are written, and a
/// reader is told what is there and then that there is no more.
#[tokio::test]
async fn a_subscriber_is_told_about_records() {
    let dir = tempfile::tempdir().unwrap();
    let port = test_port();
    let mut srv = Server::start(dir.path(), port).await;

    let c = Client::connect(config(srv.addr())).await.unwrap();

    /* Three before anyone is watching, so the replay has something. */
    for i in 0..3u64 {
        c.write(7, format!("old {i}").into_bytes(), Durability::Sync)
            .await
            .unwrap();
    }

    let mut sub = c
        .subscribe(Watch { key: None, from_seq: 1, min_seq: 0 })
        .await
        .unwrap();
    assert_eq!(sub.start(), 1);

    for i in 0..3u64 {
        let seen = sub.next().await.expect("a replayed record");
        assert_eq!(seen.seq, i + 1);
        assert_eq!(seen.partition_key, 7);
        assert_eq!(seen.data, format!("old {i}").into_bytes());
    }

    /* And then the live ones, as they land. */
    for i in 0..3u64 {
        c.write(9, format!("new {i}").into_bytes(), Durability::Sync)
            .await
            .unwrap();
        let seen = sub.next().await.expect("a live record");
        assert_eq!(seen.seq, i + 4);
        assert_eq!(seen.data, format!("new {i}").into_bytes());
    }

    srv.kill().await;
}

#[tokio::test]
async fn a_read_covers_one_key_and_ends() {
    let dir = tempfile::tempdir().unwrap();
    let port = test_port();
    let mut srv = Server::start(dir.path(), port).await;

    let c = Client::connect(config(srv.addr())).await.unwrap();
    for i in 0..6u64 {
        c.write(i % 2, format!("r{i}").into_bytes(), Durability::Sync)
            .await
            .unwrap();
    }

    /* A second client, because one connection carries one stream. */
    let reader = Client::connect(config(srv.addr())).await.unwrap();
    let mut got = Vec::new();
    let mut sub = reader
        .read(Watch { key: Some(1), ..Watch::default() })
        .await
        .unwrap();
    while let Some(r) = sub.next().await {
        assert_eq!(r.partition_key, 1);
        got.push(r.data);
    }

    assert_eq!(got.len(), 3, "one key of six alternating records");
    assert_eq!(got[0], b"r1".to_vec());

    /* Asking to be no older than a sequence the node has not reached
     * is refused rather than answered late. */
    let late = Client::connect(config(srv.addr())).await.unwrap();
    match late.subscribe(Watch { min_seq: 9_999, ..Watch::default() }).await {
        Err(Error::Server { code, .. }) => assert_eq!(code, err::BEHIND),
        other => panic!("expected BEHIND, got {other:?}"),
    }

    srv.kill().await;
}
