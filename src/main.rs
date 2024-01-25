use dotenv::dotenv;
use ethers::{
    abi::Hash,
    core::k256::ecdsa::SigningKey,
    prelude::*,
    providers::{Provider, Http},
    types::{transaction::eip2930::{AccessList, AccessListItem}, Address},
};
use eyre::Result;
use std::{
    env,
    sync::Arc,
};
use sqlx::{
    migrate::MigrateDatabase,
    Pool,
    Sqlite,
    SqlitePool,
    Row,
};
use tokio::time::{sleep, Duration};
use log;

// type ProviderType = NonceManagerMiddleware<GasEscalatorMiddleware<SignerMiddleware<Provider<Http>, Wallet<SigningKey>>>>;
type ProviderType = NonceManagerMiddleware<SignerMiddleware<Provider<Http>, Wallet<SigningKey>>>;
abigen!(PingPong, "PingPongContract.json");

async fn collect_and_insert_pings(db: &Pool<Sqlite>, contract: Arc<PingPong<ProviderType>>, start_block: u64) -> Result<()> {
    let from_block = get_last_seen_block_number(&db).await?.unwrap_or(start_block).max(start_block);
    let vec_ping_event_with_meta = get_ping_events(&contract, from_block).await?;
    log::info!("collect_and_insert_pings: {} new pings", vec_ping_event_with_meta.len());
    insert_pings(&db, vec_ping_event_with_meta).await?;

    Ok(())
}

async fn sync_pong_calls(db: &Pool<Sqlite>, provider: Arc<ProviderType>) -> Result<()> {
    let rows = get_pings_called(&db).await?;
    log::info!("sync_pong_calls: {} pongs to sync", rows.len());
    for (ping_tx_hash, optional_pong_tx_hash) in rows.into_iter() {
        let tx_status = check_tx_status(Arc::clone(&provider), optional_pong_tx_hash.unwrap()).await?;
        if tx_status {
            update_ping_status_to_completed(&db, ping_tx_hash).await?;
            log::info!("sync_pong_calls: {} ponged by {}", ping_tx_hash, optional_pong_tx_hash.unwrap());
        }
    }

    Ok(())
}

async fn send_pong_call(contract: Arc<PingPong<ProviderType>>, ping_tx_hash: Hash) -> Result<Hash> {
    let tx_raw = TransactionRequest::new()
        // .chain_id(Chain::Goerli)
        .to(contract.address())
        .data(contract.pong(ping_tx_hash.into()).calldata().unwrap())
        .value(0)
        .with_access_list(
            AccessList(
                vec![
                    AccessListItem { 
                        address: contract.address(), 
                        storage_keys: vec![], 
                    }
                ]
            )
        );

    let pending_pong_transaction = contract.client_ref().send_transaction(tx_raw, None).await?;
    let pong_pending_tx_hash = pending_pong_transaction.tx_hash();

    Ok(pong_pending_tx_hash)
}

async fn send_pong_calls(db: &Pool<Sqlite>, contract: Arc<PingPong<ProviderType>>) -> Result<()> {
    let rows = get_pings_seen(&db).await?;
    log::info!("send_pong_calls: {} pongs to call", rows.len());
    for (ping_tx_hash, _) in rows.into_iter() {
        let pong_pending_tx_hash = send_pong_call(Arc::clone(&contract), ping_tx_hash).await?;

        update_ping_status_to_called(
            &db,
            ping_tx_hash,
            pong_pending_tx_hash,
        ).await?;

        log::info!("send_pong_calls: {} ping called by pong {}", ping_tx_hash, pong_pending_tx_hash);
    }

    Ok(())
}

async fn loopy(db: &Pool<Sqlite>, contract: Arc<PingPong<ProviderType>>, provider: Arc<ProviderType>, start_block: u64, sleep_duration_in_secs: u64) -> Result<()> {
    // get new ping txs
    collect_and_insert_pings(&db, Arc::clone(&contract), start_block).await?;
    // send pong txs
    send_pong_calls(&db, Arc::clone(&contract)).await?;

    // sleep
    log::info!("sleeping for {sleep_duration_in_secs} seconds");
    sleep(Duration::from_secs(sleep_duration_in_secs)).await;

    // sync rows with status called
    sync_pong_calls(&db, Arc::clone(&provider)).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    dotenv().ok();

    // get env vars and parse into expected type if necessary
    let contract_address = env::var("CONTRACT_ADDRESS").expect("'CONTRACT_ADDRESS' is not set").parse::<Address>()?;
    let db_url = env::var("DB_URL").expect("'DB_URL' is not set");
    let start_block = env::var("START_BLOCK").unwrap_or("0".to_string()).parse::<u64>()?;
    let goerli_https_rpc_url = env::var("GOERLI_HTTPS_RPC_URL").expect("'GOERLI_HTTPS_RPC_URL' is not set");
    let private_key = env::var("PRIVATE_KEY").expect("'PRIVATE_KEY' is not set");
    const SLEEP_DURATION_IN_SECS: u64 = 60;

    // create signer, provider and contract
    let signer = private_key.parse::<LocalWallet>()?.with_chain_id(Chain::Goerli);
    let provider = setup_provider(goerli_https_rpc_url, signer).await?;
    let contract = create_contract_instance(Arc::clone(&provider), contract_address).await?;

    // create db if not exists
    create_db_if_not_exists(&db_url).await?;
    let db = get_db(&db_url).await?;

    // create table if not exists
    create_table_if_not_exists(&db).await?;

    // sync rows with status called
    sync_pong_calls(&db, Arc::clone(&provider)).await?;

    loop {
        match loopy(&db, Arc::clone(&contract), Arc::clone(&provider), start_block, SLEEP_DURATION_IN_SECS).await {
            Ok(_) => {},
            Err(e) => {
                log::info!("{e}");
                log::info!("sleeping for {SLEEP_DURATION_IN_SECS} seconds");
                sleep(Duration::from_secs(SLEEP_DURATION_IN_SECS)).await;
            },
        }
    }

    // Ok(())
}

async fn setup_provider(goerli_https_rpc_url: String, signer: Wallet<SigningKey>) -> Result<Arc<ProviderType>> {
    let signer_address = signer.address();
    // vanilla provider
    let provider = Provider::<Http>::try_connect(&goerli_https_rpc_url).await?;
    // Signer middleware
    let provider = SignerMiddleware::new_with_provider_chain(provider, signer).await?;
    // // GasEscalator middleware
    // let escalator = GeometricGasPrice::new(1.125_f64, 30_u64, None::<u64>);
    // let provider = GasEscalatorMiddleware::new(provider, escalator, Frequency::PerBlock);
    // NonceManager middleware
    let provider = NonceManagerMiddleware::new(provider, signer_address);

    Ok(Arc::new(provider))
}

async fn create_contract_instance(provider: Arc<ProviderType>, contract_address: Address) -> Result<Arc<PingPong<ProviderType>>> {
    let contract = PingPong::new(contract_address, Arc::clone(&provider));

    Ok(Arc::new(contract))
}

async fn check_tx_status(provider: Arc<ProviderType>, tx_hash: Hash) -> Result<bool> {
    let res = match provider.get_transaction_receipt(tx_hash).await? {
        Some(receipt) => {
            match receipt.status {
                Some(s) => s.as_u64() == 1,
                None => receipt.logs.len() > 0,
            }
        },
        None => false,
    };

    Ok(res)
}

async fn get_ping_events<T>(contract: &PingPong<ProviderType>, from_block: T) -> Result<Vec<(PingFilter, LogMeta)>>
    where T: Into<BlockNumber>
{
    let ping_event_type = contract.event::<ping_pong::PingFilter>();
    let vec_ping_event_with_meta = ping_event_type.from_block(from_block).address(ValueOrArray::Value(contract.address())).query_with_meta().await?;

    Ok(vec_ping_event_with_meta)
}

async fn create_db_if_not_exists(db_url: &str) -> Result<()> {
    if !Sqlite::database_exists(db_url).await.unwrap_or(false) {
        log::info!("Creating database {}", db_url);
        match Sqlite::create_database(db_url).await {
            Ok(_) => log::info!("Create db success"),
            Err(error) => panic!("error: {}", error),
        }
    } else {
        log::info!("Database already exists");
    }

    Ok(())
}

async fn get_db(db_url: &str) -> Result<Pool<Sqlite>> {
    let db = SqlitePool::connect(db_url).await?;

    Ok(db)
}

async fn create_table_if_not_exists(db: &Pool<Sqlite>) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS ping
            (
                txHash VARCHAR(66) PRIMARY KEY NOT NULL,
                blockNumber INTEGER NOT NULL,
                status TINYINT NOT NULL,
                pongTxHash VARCHAR(66)
            );
        "#
    ).execute(db).await?;

    Ok(())
}

async fn insert_pings(db: &Pool<Sqlite>, data: Vec<(PingFilter, LogMeta)>) -> Result<()> {
    for (_, meta) in data.into_iter() {
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO ping (txHash, blockNumber, status)
            VALUES ($1, $2, $3)
            "#
        )
            .bind(format!("0x{:X}", meta.transaction_hash))
            .bind(meta.block_number.as_u32())
            .bind(0_u8)
            .execute(db).await?;
    }

    Ok(())
}

async fn get_pings_by_status(db: &Pool<Sqlite>, status: u8) -> Result<Vec<(Hash, Option<Hash>)>> {
    let result = sqlx::query(
        r#"
        SELECT
            txHash,
            pongTxHash
        FROM ping
        WHERE
            status = $1
        ORDER BY
            blockNumber ASC
        "#
    )
        .bind(status)
        .fetch_all(db).await?;

    let out = result
        .iter()
        .map(|r| (
            r.get::<String, _>("txHash").parse::<Hash>().unwrap(),
            r.get::<String, _>("pongTxHash").parse::<Hash>().ok(),
        ))
        .collect::<Vec<_>>();

    Ok(out)
}

async fn get_pings_seen(db: &Pool<Sqlite>) -> Result<Vec<(Hash, Option<Hash>)>> {
    get_pings_by_status(db, 0).await
}

async fn get_pings_called(db: &Pool<Sqlite>) -> Result<Vec<(Hash, Option<Hash>)>> {
    get_pings_by_status(db, 1).await
}

async fn _get_pings_completed(db: &Pool<Sqlite>) -> Result<Vec<(Hash, Option<Hash>)>> {
    get_pings_by_status(db, 2).await
}

async fn update_ping_status_to_called(db: &Pool<Sqlite>, ping_tx_hash: Hash, pong_tx_hash: Hash) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE ping
        SET status = 1, pongTxHash = $2
        WHERE ping.txHash = $1
        "#
    )
        .bind(format!("0x{:X}", ping_tx_hash))
        .bind(format!("0x{:X}", pong_tx_hash))
        .execute(db).await?;

    Ok(())
}

async fn update_ping_status_to_completed(db: &Pool<Sqlite>, ping_tx_hash: Hash) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE ping
        SET status = 2
        WHERE ping.txHash = $1
        "#
    )
        .bind(format!("0x{:X}", ping_tx_hash))
        .execute(db).await?;

    Ok(())
}

async fn get_last_seen_block_number(db: &Pool<Sqlite>) -> Result<Option<u64>> {
    let result = sqlx::query(
        r#"
        SELECT
            MAX(blockNumber) as lastBlockNumber
        FROM ping
        "#
    )
        .fetch_one(db).await;

    let res = match result {
        Ok(row) => Some(row.get::<u32, _>("lastBlockNumber") as u64),
        Err(_) => None,
    };

    Ok(res)
}
