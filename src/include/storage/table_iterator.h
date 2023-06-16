#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator {
public:
  // you may define your own constructor based on your member variables
  explicit TableIterator(RowId rowId,TableHeap *tableheap);

  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator operator++(int);

public:
    Row *ite_row;
    TableHeap* ite_tableheap;
};

#endif  // MINISQL_TABLE_ITERATOR_H
