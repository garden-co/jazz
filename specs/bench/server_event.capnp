@0xbbfcf5f3abac42e1;

struct ObjectUpdatedPayload {
  objectId @0 :Text;
  metadata @1 :Text;
  branchName @2 :Text;
  commits @3 :List(Text);
}

struct SyncPayload {
  objectUpdated @0 :ObjectUpdatedPayload;
}

struct ServerEvent {
  type @0 :Text;
  payload @1 :SyncPayload;
}
