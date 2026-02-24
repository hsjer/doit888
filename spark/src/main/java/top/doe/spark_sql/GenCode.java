package top.doe.spark_sql;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import scala.collection.Iterator;

public class GenCode {

    /* 001 */
    public Object generate(Object[] references) {
        /* 002 */
        return new GeneratedIteratorForCodegenStage2(references);
        /* 003 */
    }

    /* 004 */
    /* 005 */ // codegenStageId=2@
    /* 006 */ final class GeneratedIteratorForCodegenStage2 extends org.apache.spark.sql.execution.BufferedRowIterator {
        /* 007 */   private Object[] references;
        /* 008 */   private scala.collection.Iterator[] inputs;
        /* 009 */   private boolean sort_needToSort_0;
        /* 010 */   private org.apache.spark.sql.execution.UnsafeExternalRowSorter sort_sorter_0;
        /* 011 */   private org.apache.spark.executor.TaskMetrics sort_metrics_0;
        /* 012 */   private Iterator<UnsafeRow> sort_sortedIter_0;
        /* 013 */   private scala.collection.Iterator inputadapter_input_0;

        /* 014 */
        /* 015 */
        public GeneratedIteratorForCodegenStage2(Object[] references) {
            /* 016 */
            this.references = references;
            /* 017 */
        }

        /* 018 */
        /* 019 */
        public void init(int index, scala.collection.Iterator[] inputs) {
            /* 020 */
            partitionIndex = index;
            /* 021 */
            this.inputs = inputs;
            /* 022 */
            sort_needToSort_0 = true;
            /* 023 */  // UnsafeExternalRowSorter
            sort_sorter_0 = ((org.apache.spark.sql.execution.SortExec) references[0] /* plan */).createSorter();
            /* 024 */
            sort_metrics_0 = org.apache.spark.TaskContext.get().taskMetrics();
            /* 025 */
            /* 026 */
            inputadapter_input_0 = inputs[0];
            /* 027 */
            /* 028 */
        }

        /* 029 */
        /* 030 */
        private void sort_addToSorter_0() throws java.io.IOException {
            /* 031 */
            while (inputadapter_input_0.hasNext()) {
                /* 032 */
                InternalRow inputadapter_row_0 = (InternalRow) inputadapter_input_0.next();
                /* 033 */
                /* 034 */
                sort_sorter_0.insertRow((UnsafeRow) inputadapter_row_0);
                /* 035 */       // shouldStop check is eliminated
                /* 036 */
            }
            /* 037 */
            /* 038 */
        }

        /* 039 */
        /* 040 */
        protected void processNext() throws java.io.IOException {
            /* 041 */
            if (sort_needToSort_0) {
                /* 042 */
                long sort_spillSizeBefore_0 = sort_metrics_0.memoryBytesSpilled();
                /* 043 */
                sort_addToSorter_0();
                /* 044 */
                //sort_sortedIter_0 = sort_sorter_0.sort();
                /* 045 */
                ((org.apache.spark.sql.execution.metric.SQLMetric) references[3] /* sortTime */).add(sort_sorter_0.getSortTimeNanos() / 1000000);
                /* 046 */
                ((org.apache.spark.sql.execution.metric.SQLMetric) references[1] /* peakMemory */).add(sort_sorter_0.getPeakMemoryUsage());
                /* 047 */
                ((org.apache.spark.sql.execution.metric.SQLMetric) references[2] /* spillSize */).add(sort_metrics_0.memoryBytesSpilled() - sort_spillSizeBefore_0);
                /* 048 */
                sort_metrics_0.incPeakExecutionMemory(sort_sorter_0.getPeakMemoryUsage());
                /* 049 */
                sort_needToSort_0 = false;
                /* 050 */
            }
            /* 051 */
            /* 052 */
            while (sort_sortedIter_0.hasNext()) {
                /* 053 */
                UnsafeRow sort_outputRow_0 = (UnsafeRow) sort_sortedIter_0.next();
                /* 054 */
                /* 055 */
                append(sort_outputRow_0);
                /* 056 */
                /* 057 */
                if (shouldStop()) return;
                /* 058 */
            }
            /* 059 */
        }
        /* 060 */
        /* 061 */
    }
}
