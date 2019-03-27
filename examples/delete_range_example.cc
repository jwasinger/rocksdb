// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include <cstdio>
#include <string>
#include <vector>
#include <iostream>
#include <chrono>
#include <ratio>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace rocksdb;
using namespace std::chrono;

std::string kDBPath = "/tmp/rocksdb_column_families_example";

int main() {
  // open DB
  Options options;
  options.create_if_missing = true;
  DB* db;
  Status s = DB::Open(options, kDBPath, &db);
  high_resolution_clock::time_point t1;
  high_resolution_clock::time_point t2;

  assert(s.ok());

  // close DB
//  delete cf;
  delete db;

  // open DB with two column families
  std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  column_families.push_back(ColumnFamilyDescriptor(
      kDefaultColumnFamilyName, ColumnFamilyOptions()));

  std::vector<ColumnFamilyHandle*> handles;
  s = DB::Open(DBOptions(), kDBPath, column_families, &handles, &db);
  assert(s.ok());

  // put and get from non-default column family
  for (int i = 0; i < 128; i++) {
    s = db->Put(WriteOptions(), handles[0], Slice("key"+std::to_string(i)), Slice("value"));
    assert(s.ok());
  }

  std::string value;
  s = db->Get(ReadOptions(), handles[0], Slice("key1"), &value);
  assert(s.ok());

  s = db->DeleteRange(WriteOptions(), handles[0], Slice("key0"), Slice("key10"));
  assert(s.ok());

  t1 = high_resolution_clock::now();
  for (int i = 20; i < 128; i++) {
    s = db->Get(ReadOptions(), handles[0], Slice("key"+std::to_string(i)), &value);
    assert(s.ok());
  }
  t2 = high_resolution_clock::now();

  duration<double> timespan = duration_cast<duration<double>>(t2 - t1);
  std::cout << timespan.count() << " seconds" << std::endl;

  // atomic write
  WriteBatch batch;
  batch.Put(handles[0], Slice("key2"), Slice("value2"));
  batch.Delete(handles[0], Slice("key"));
  s = db->Write(WriteOptions(), &batch);
  assert(s.ok());

/*
  // drop column family
  s = db->DropColumnFamily(handles[1]);
  assert(s.ok());
*/

  // close db
  for (auto handle : handles) {
    delete handle;
  }
  delete db;

  return 0;
}
