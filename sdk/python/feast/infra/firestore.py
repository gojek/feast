from typing import List

from feast import FeatureTable
from feast.infra.provider import Provider


def _table_id(project: str, table: FeatureTable) -> str:
    return f"{project}_{table.name}"


def _delete_collection(coll_ref, batch_size=1000) -> None:
    """
    Delete Firebase collection. There is no way to delete the entire collections, so we have to
    delete documents in the collection one by one.
    """
    while True:
        docs = coll_ref.limit(batch_size).stream()
        deleted = 0

        for doc in docs:
            doc.reference.delete()
            deleted = deleted + 1

        if deleted < batch_size:
            return


class Firestore(Provider):
    def update_infra(
        self,
        project: str,
        tables_to_delete: List[FeatureTable],
        tables_to_keep: List[FeatureTable],
    ):
        import firebase_admin
        from firebase_admin import firestore

        firebase_admin.initialize_app()
        db = firestore.client()

        table_id = lambda t: _table_id(project, table=t)

        for table in tables_to_keep:
            db.collection(project).document(table_id(table)).set(
                {"created_at": firestore.SERVER_TIMESTAMP}
            )

        for table in tables_to_delete:
            _delete_collection(
                db.collection(project).document(table_id(table)).collection("values")
            )
            db.collection(project).document(table_id(table)).delete()

    def teardown_infra(self, project: str, tables: List[FeatureTable]) -> None:
        import firebase_admin
        from firebase_admin import firestore

        firebase_admin.initialize_app()
        db = firestore.client()
        table_id = lambda t: _table_id(project, table=t)
        for table in tables:
            _delete_collection(
                db.collection(project).document(table_id(table)).collection("values")
            )
            db.collection(project).document(table_id(table)).delete()
