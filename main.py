from pymongo import MongoClient
import logging
from tqdm import tqdm
import time
from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure, OperationFailure

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transfer_entire_database():
    source_uri = "mongodb://source_user:source_password@source_host:27017/admin"
    source_client = MongoClient(
        source_uri,
        serverSelectionTimeoutMS=30000,
        socketTimeoutMS=0,
        connectTimeoutMS=30000,
        maxPoolSize=10,
        retryWrites=True
    )
    source_db = source_client["StockMarket"]
    
    destination_uri = "mongodb://dest_user:dest_password@dest_host:27017/admin"
    destination_client = MongoClient(
        destination_uri,
        serverSelectionTimeoutMS=30000,
        socketTimeoutMS=0,
        connectTimeoutMS=30000,
        maxPoolSize=10,
        retryWrites=True
    )
    destination_db = destination_client["StockMarket"]
    
    try:
        collection_names = source_db.list_collection_names()
        logger.info(f"Found {len(collection_names)} collections to transfer")
        
        for collection_name in collection_names:
            logger.info(f"Starting transfer of collection: {collection_name}")
            
            source_collection = source_db[collection_name]
            destination_collection = destination_db[collection_name]
            
            total_docs = source_collection.count_documents({})
            existing_docs = destination_collection.count_documents({})
            
            logger.info(f"Source documents: {total_docs}, Existing in destination: {existing_docs}")
            
            if total_docs == 0:
                logger.info(f"No documents found in {collection_name}")
                continue
            
            if existing_docs >= total_docs:
                logger.info(f"Collection {collection_name} already fully transferred, skipping")
                continue
            
            batch_size = 500
            transferred = existing_docs
            remaining_docs = total_docs - existing_docs
            
            with tqdm(total=remaining_docs, desc=f"Transferring {collection_name}") as pbar:
                while transferred < total_docs:
                    retry_count = 0
                    max_retries = 3
                    
                    while retry_count < max_retries:
                        try:
                            cursor = source_collection.find().skip(transferred).limit(batch_size)
                            batch = list(cursor)
                            
                            if not batch:
                                break
                            
                            if batch:
                                try:
                                    destination_collection.insert_many(batch, ordered=False)
                                except Exception as insert_error:
                                    for doc in batch:
                                        try:
                                            destination_collection.insert_one(doc)
                                        except:
                                            pass
                                
                                transferred += len(batch)
                                pbar.update(len(batch))
                            
                            break
                            
                        except (ServerSelectionTimeoutError, ConnectionFailure, OperationFailure) as e:
                            retry_count += 1
                            logger.warning(f"Attempt {retry_count} failed for batch starting at {transferred}: {str(e)}")
                            
                            if retry_count < max_retries:
                                logger.info(f"Retrying in 2 seconds...")
                                time.sleep(2)
                                
                                try:
                                    source_client.close()
                                    destination_client.close()
                                    
                                    source_client = MongoClient(
                                        source_uri,
                                        serverSelectionTimeoutMS=30000,
                                        socketTimeoutMS=0,
                                        connectTimeoutMS=30000,
                                        maxPoolSize=10
                                    )
                                    destination_client = MongoClient(
                                        destination_uri,
                                        serverSelectionTimeoutMS=30000,
                                        socketTimeoutMS=0,
                                        connectTimeoutMS=30000,
                                        maxPoolSize=10
                                    )
                                    
                                    source_db = source_client["StockMarket"]
                                    destination_db = destination_client["StockMarket"]
                                    source_collection = source_db[collection_name]
                                    destination_collection = destination_db[collection_name]
                                    
                                except Exception as reconnect_error:
                                    logger.error(f"Reconnection failed: {str(reconnect_error)}")
                            else:
                                logger.error(f"Max retries exceeded for batch starting at {transferred}")
                                raise
                    
                    time.sleep(0.05)
            
            logger.info(f"Completed transfer of {collection_name}")
            
            dest_count = destination_collection.count_documents({})
            logger.info(f"Verification - Source: {total_docs}, Destination: {dest_count}")
    
    except Exception as e:
        logger.error(f"Error during transfer: {str(e)}")
        raise
    
    finally:
        source_client.close()
        destination_client.close()
        logger.info("Transfer completed and connections closed")

def transfer_with_resume(start_collection="", start_from=0):
    source_uri = "mongodb://source_user:source_password@source_host:27017/admin"
    source_client = MongoClient(
        source_uri,
        serverSelectionTimeoutMS=30000,
        socketTimeoutMS=0,
        connectTimeoutMS=30000,
        maxPoolSize=10
    )
    source_db = source_client["StockMarket"]
    
    destination_uri = "mongodb://dest_user:dest_password@dest_host:27017/admin"
    destination_client = MongoClient(
        destination_uri,
        serverSelectionTimeoutMS=30000,
        socketTimeoutMS=0,
        connectTimeoutMS=30000,
        maxPoolSize=10
    )
    destination_db = destination_client["StockMarket"]
    
    try:
        collection_names = source_db.list_collection_names()
        
        if start_collection:
            try:
                start_index = collection_names.index(start_collection)
                collection_names = collection_names[start_index:]
            except ValueError:
                logger.warning(f"Collection {start_collection} not found, starting from beginning")
        
        for i, collection_name in enumerate(collection_names):
            source_collection = source_db[collection_name]
            destination_collection = destination_db[collection_name]
            
            total_docs = source_collection.count_documents({})
            existing_docs = destination_collection.count_documents({})
            
            current_start = max(existing_docs, start_from if i == 0 and start_collection else 0)
            
            logger.info(f"Collection: {collection_name}")
            logger.info(f"Source: {total_docs}, Existing: {existing_docs}, Starting from: {current_start}")
            
            if current_start >= total_docs:
                logger.info(f"Collection {collection_name} already complete, skipping")
                continue
            
            remaining_docs = total_docs - current_start
            batch_size = 500
            transferred = current_start
            
            with tqdm(total=remaining_docs, desc=f"Resuming {collection_name}") as pbar:
                while transferred < total_docs:
                    try:
                        cursor = source_collection.find().skip(transferred).limit(batch_size).max_time_ms(300000)
                        batch = list(cursor)
                        
                        if not batch:
                            break
                        
                        try:
                            destination_collection.insert_many(batch, ordered=False)
                        except Exception as insert_error:
                            for doc in batch:
                                try:
                                    destination_collection.insert_one(doc)
                                except:
                                    pass
                        
                        transferred += len(batch)
                        pbar.update(len(batch))
                        
                        if transferred % 2000 == 0:
                            logger.info(f"Progress checkpoint: {transferred}/{total_docs} documents transferred")
                        
                        time.sleep(0.02)
                        
                    except Exception as e:
                        logger.error(f"Error at document {transferred}: {str(e)}")
                        logger.info(f"Resume from: transfer_with_resume('{collection_name}', {transferred})")
                        raise
            
            logger.info(f"Completed transfer of {collection_name}")
            
            final_count = destination_collection.count_documents({})
            logger.info(f"Final verification - Source: {total_docs}, Destination: {final_count}")
    
    except Exception as e:
        logger.error(f"Transfer failed: {str(e)}")
        raise
    
    finally:
        source_client.close()
        destination_client.close()

if __name__ == "__main__":
    print("Choose transfer method:")
    print("1. Transfer entire database")
    print("2. Resume transfer from specific collection/document")
    
    choice = input("Enter choice (1 or 2): ")
    
    if choice == "1":
        transfer_entire_database()
    elif choice == "2":
        collection_name = input("Enter collection name to resume from (leave empty for all): ")
        start_from = int(input("Enter document number to resume from (default 0): ") or 0)
        transfer_with_resume(collection_name, start_from)
    else:
        print("Invalid choice")