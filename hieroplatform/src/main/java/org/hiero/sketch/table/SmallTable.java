package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;

import java.io.Serializable;

/**
 * A SmallTable is similar to a Table, but it is intended to be shipped over the network.
 * We expect all columns to be serializable.
 */
public class SmallTable
        extends BaseTable
        implements Serializable {
    @NonNull
    protected final Schema schema;
    protected final int rowCount;

    @Override
    public Schema getSchema() {
        return this.schema;
    }

    public SmallTable(@NonNull final Iterable<IColumn> columns) {
        super(columns);
        this.rowCount = BaseTable.columnSize(this.columns.values());
        final Schema s = new Schema();
        for (final IColumn c : columns) {
            s.append(c.getDescription());
            if (!(c instanceof Serializable))
                throw new RuntimeException("Column for SmallTable is not serializable");
        }
        this.schema = s;
    }

    public SmallTable(@NonNull final Schema schema) {
        super(schema);
        this.schema = schema;
        this.rowCount = 0;
    }

    @Override
    public IRowIterator getRowIterator() {
        return new FullMembership.FullMembershipIterator(this.rowCount);
    }

    @Override
    public @NonNull IMembershipSet getMembershipSet() {
        return new FullMembership(this.rowCount);
    }

    @Override
    public int getNumOfRows() {
        return this.rowCount;
    }
}