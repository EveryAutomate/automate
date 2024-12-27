from typing import Dict, List, Optional, Union
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from .IDataStore import DataStore

class FirestoreDataStore(DataStore):

    def __init__(self, client: firestore.Client):
        self.db = client

    def read(self, collection: str, identifier: Optional[str] = None, filters: Optional[List] = None) -> List[Dict]:
        collection_ref = self.db.collection(collection)
        
        # If specific document ID provided, fetch just that document
        if identifier:
            doc = collection_ref.document(identifier).get()
            return [doc.to_dict()] if doc.exists else []
        
        # Apply filters if provided, otherwise get all documents
        if filters:
            query = collection_ref
            for field, operator, value in filters:
                query = query.where(filter=FieldFilter(field, operator, value))
            docs = query.stream()
        else:
            docs = collection_ref.stream()
            
        return [doc.to_dict() for doc in docs]

    def write(self, collection: str, data: Union[Dict, List[Dict]]) -> None:

        collection_ref = self.db.collection(collection)
        batch = self.db.batch()
        
        # Ensure data is a list of documents
        documents = [data] if isinstance(data, dict) else data
        document_tags_seen = set()
        
        for document in documents:
            document_tag = document.get("document_tag")
            contents = document.get("contents")
            
            # Validate required fields
            if not document_tag or not contents:
                raise ValueError("Each document must have 'document_tag' and 'contents'.")
                
            # Check for duplicate document tags
            if document_tag in document_tags_seen:
                raise ValueError(f"Duplicate document tag '{document_tag}'")
                
            document_tags_seen.add(document_tag)
            doc_ref = collection_ref.document(document_tag)
            
            # Prevent overwriting existing documents
            if doc_ref.get().exists:
                raise ValueError(f"Document '{document_tag}' already exists")
                
            batch.set(doc_ref, contents)
            
        batch.commit()

    def update(self, collection: str, identifier: str, updates: Dict) -> None:

        doc_ref = self.db.collection(collection).document(identifier)
        if not doc_ref.get().exists:
            raise ValueError(f"Document '{identifier}' does not exist")
        doc_ref.update(updates)

    def delete(self, collection: str, identifier: Optional[str] = None, field: Optional[str] = None) -> None:

        collection_ref = self.db.collection(collection)
        
        if identifier and field:
            # Delete specific field from document
            doc_ref = collection_ref.document(identifier)
            doc_ref.update({field: firestore.DELETE_FIELD})
        elif identifier:
            # Delete entire document
            collection_ref.document(identifier).delete()
        else:
            # Delete entire collection
            self._delete_collection(collection_ref)
            
    def _delete_collection(self, coll_ref, batch_size: int = 10) -> None:

        docs = coll_ref.limit(batch_size).stream()
        deleted = 0
        
        batch = self.db.batch()
        for doc in docs:
            batch.delete(doc.reference)
            deleted += 1
            
        if deleted >= batch_size:
            self._delete_collection(coll_ref, batch_size)