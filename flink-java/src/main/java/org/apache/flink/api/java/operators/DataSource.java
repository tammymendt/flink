/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.operators;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.OperatorInformation;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;

/**
 * An operation that creates a new data set (data source). The operation acts as the
 * data set on which to apply further transformations. It encapsulates additional
 * configuration parameters, to customize the execution.
 * 
 * @param <OUT> The type of the elements produced by this data source.
 */
public class DataSource<OUT> extends Operator<OUT, DataSource<OUT>> {

	private final InputFormat<OUT, ?> inputFormat;

	private final String dataSourceLocationName;

	private Configuration parameters;

	private int[] splitPartitionKeys;

	private Partitioner<OUT> splitPartitioner;

	private int[] splitGroupKeys;

	private Ordering splitOrdering;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new data source.
	 * 
	 * @param context The environment in which the data source gets executed.
	 * @param inputFormat The input format that the data source executes.
	 * @param type The type of the elements produced by this input format.
	 */
	public DataSource(ExecutionEnvironment context, InputFormat<OUT, ?> inputFormat, TypeInformation<OUT> type, String dataSourceLocationName) {
		super(context, type);
		
		this.dataSourceLocationName = dataSourceLocationName;
		
		if (inputFormat == null) {
			throw new IllegalArgumentException("The input format may not be null.");
		}
		
		this.inputFormat = inputFormat;
		
		if (inputFormat instanceof NonParallelInput) {
			this.dop = 1;
		}
	}

	/**
	 * Gets the input format that is executed by this data source.
	 * 
	 * @return The input format that is executed by this data source.
	 */
	public InputFormat<OUT, ?> getInputFormat() {
		return this.inputFormat;
	}
	
	/**
	 * Pass a configuration to the InputFormat
	 * @param parameters Configuration parameters
	 */
	public DataSource<OUT> withParameters(Configuration parameters) {
		this.parameters = parameters;
		return this;
	}
	
	/**
	 * @return Configuration for the InputFormat.
	 */
	public Configuration getParameters() {
		return this.parameters;
	}

	/**
	 * Defines this data source as partitioned on the fields defined by field positions.
	 * This requires that the data of the input format is partitioned across input splits, i.e,
	 * all records sharing the same key (combination) are contained in a single input split.
	 * <br>
	 * <b>IMPORTANT: Marking a data source as partitioned that does not fulfill this requirement might
	 * produce wrong results!
	 * </b>
	 *
	 * @param partitionKeys The field positions of the partitioning keys.
	 * @return This DataSource with the annotated partitioning information.
	 */
	public DataSource<OUT> partitionedBy(int[] partitionKeys) {
		return this.partitionedBy(partitionKeys, null);
	}

	/**
	 * Defines this data source as partitioned on the fields defined by field positions.
	 * This requires that the data of the input format is partitioned across input splits, i.e,
	 * all records sharing the same key (combination) are contained in a single input split.
	 * <br>
	 * <b>IMPORTANT: Marking a data source as partitioned that does not fulfill this requirement might
	 * produce wrong results!
	 * </b>
	 *
	 * @param partitionKeys The field positions of the partitioning keys.
	 * @param partitionMethodId An ID for the method used to partition the data of the source.
	 * @return This DataSource with the annotated partitioning information.
	 */
	public DataSource<OUT> partitionedBy(int[] partitionKeys, String partitionMethodId) {

		if(partitionKeys == null) {
			throw new InvalidProgramException("PartitionKeys may not be null.");
		} else if (partitionKeys.length == 0) {
			throw new InvalidProgramException("PartitionKeys may not be empty.");
		}

		if(!(this.getType() instanceof TupleTypeInfo)) {
			throw new InvalidProgramException("Field position keys may only be defined for tuple data types");
		}

		this.splitPartitionKeys = getAllFlatKeys(partitionKeys);
		if(partitionMethodId != null) {
			this.splitPartitioner = new SourcePartitionerMarker<OUT>(partitionMethodId);
		}
		else {
			this.splitPartitioner = null;
		}

		return this;
	}

	/**
	 * Defines this data source as partitioned on the fields defined by field expressions.
	 * This requires that the data of the input format is partitioned across input splits, i.e,
	 * all records sharing the same key (combination) are contained in a single input split.
	 * <br>
	 * <b>IMPORTANT: Marking a data source as partitioned that does not fulfill this requirement might
	 * produce wrong results!
	 * </b>
	 *
	 * @param partitionKeys The field expressions referencing the partitioning keys.
	 * @return This DataSource with the annotated partitioning information.
	 */
	public DataSource<OUT> partitionedBy(String[] partitionKeys) {
		return this.partitionedBy(partitionKeys, null);
	}

	/**
	 * Defines this data source as partitioned on the fields defined by field expressions.
	 * This requires that the data of the input format is partitioned across input splits, i.e,
	 * all records sharing the same key (combination) are contained in a single input split.
	 * <br>
	 * <b>IMPORTANT: Marking a data source as partitioned that does not fulfill this requirement might
	 * produce wrong results!
	 * </b>
	 *
	 * @param partitionKeys The field expressions referencing the partitioning keys.
	 * @param partitionMethodId An ID for the method used to partition the data of the source.
	 * @return This DataSource with the annotated partitioning information.
	 */
	public DataSource<OUT> partitionedBy(String[] partitionKeys, String partitionMethodId) {

		if(partitionKeys == null) {
			throw new InvalidProgramException("PartitionKeys may not be null.");
		} else if (partitionKeys.length == 0) {
			throw new InvalidProgramException("PartitionKeys may not be empty.");
		}

		this.splitPartitionKeys = getAllFlatKeys(partitionKeys);
		if(partitionMethodId != null) {
			this.splitPartitioner = new SourcePartitionerMarker<OUT>(partitionMethodId);
		}
		else {
			this.splitPartitioner = null;
		}
		return this;
	}

	/**
	 * Defines the InputSplits of the InputFormat of this data source as grouped on the
	 * fields defined by the field positions.
	 * This requires that the data of the input format is grouped within each input split, i.e.,
	 * all records sharing the same key (combination) are emitted as a single sequence by the input
	 * format for each input split.
	 * <br>
	 * <b>IMPORTANT: Marking a data source as grouped that does not fulfill this requirement might
	 * produce wrong results!
	 * </b>
	 *
	 * @param groupKeys The field positions of the grouping keys.
	 * @return This DataSource with annotated grouping information.
	 */
	public DataSource<OUT> splitsGroupedBy(int[] groupKeys) {

		if(groupKeys == null) {
			throw new InvalidProgramException("GroupKeys may not be null.");
		} else if (groupKeys.length == 0) {
			throw new InvalidProgramException("GroupKeys may not be empty.");
		}

		if(this.splitOrdering != null) {
			throw new InvalidProgramException("DataSource may either be grouped or sorted.");
		}

		this.splitGroupKeys = getAllFlatKeys(groupKeys);

		return this;
	}

	/**
	 * Defines the InputSplits of the InputFormat of this data source as grouped on the
	 * fields defined by the field expressions.
	 * This requires that the data of the input format is grouped within each input split, i.e.,
	 * all records sharing the same key (combination) are emitted as a single sequence by the input
	 * format for each input split.
	 * <br>
	 * <b>IMPORTANT: Marking a data source as grouped that does not fulfill this requirement might
	 * produce wrong results!
	 * </b>
	 *
	 * @param groupKeys The field expressions referencing the grouping keys.
	 * @return This DataSource with annotated grouping information.
	 */
	public DataSource<OUT> splitsGroupedBy(String[] groupKeys) {

		if(groupKeys == null) {
			throw new InvalidProgramException("GroupKeys may not be null.");
		} else if (groupKeys.length == 0) {
			throw new InvalidProgramException("GroupKeys may not be empty.");
		}

		if(this.splitOrdering != null) {
			throw new InvalidProgramException("DataSource may either be grouped or sorted.");
		}

		this.splitGroupKeys = getAllFlatKeys(groupKeys);

		return this;
	}


	/**
	 * Defines the InputSplits of the InputFormat of this data source as ordered on the fields
	 * defined by the field positions and the specified orders.
	 * This requires that the data of the input format is ordered within each input split in the
	 * specified order on the specified keys.
	 * <br>
	 * <b>IMPORTANT: Marking a data source as ordered that does not fulfill this requirement might
	 * produce wrong results!
	 * </b>
	 *
	 * @param orderKeys The field positions referencing the grouping keys.
	 * @param orders
	 * @return This DataSource with annotated grouping information.
	 */
	public DataSource<OUT> splitsOrderedBy(int[] orderKeys, Order[] orders) {

		if(orderKeys == null || orders == null) {
			throw new InvalidProgramException("OrderKeys or Orders may not be null.");
		} else if (orderKeys.length == 0) {
			throw new InvalidProgramException("OrderKeys may not be empty.");
		} else if (orders.length == 0) {
			throw new InvalidProgramException("Orders may not be empty");
		} else if (orderKeys.length != orders.length) {
			throw new InvalidProgramException("Number of OrderKeys and Orders must match.");
		}

		if(this.splitGroupKeys != null) {
			throw new InvalidProgramException("DataSource may either be grouped or sorted.");
		}

		this.splitOrdering = new Ordering();

		for(int i=0; i<orderKeys.length; i++) {
			int pos = orderKeys[i];
			int[] flatKeys = this.getAllFlatKeys(new int[]{pos});

			for(int key : flatKeys) {
				// check for duplicates
				for (int okey : splitOrdering.getFieldPositions()) {
					if (key == okey) {
						throw new InvalidProgramException("Duplicate key in partition key expression " + pos);
					}
				}
				// append key
				this.splitOrdering.appendOrdering(key, null, orders[i] );
			}
		}

		return this;
	}

	/**
	 * Defines the InputSplits of the InputFormat of this data source as ordered on the fields
	 * defined by the field expressions and the specified orders.
	 * This requires that the data of the input format is ordered within each input split in the
	 * specified order on the specified keys.
	 * <br>
	 * <b>IMPORTANT: Marking a data source as ordered that does not fulfill this requirement might
	 * produce wrong results!
	 * </b>
	 *
	 * @param orderKeys The field expressions referencing the grouping keys.
	 * @param orders
	 * @return This DataSource with annotated grouping information.
	 */
	public DataSource<OUT> splitsOrderedBy(String[] orderKeys, Order[] orders) {

		if(orderKeys == null || orders == null) {
			throw new InvalidProgramException("OrderKeys or Orders may not be null.");
		} else if (orderKeys.length == 0) {
			throw new InvalidProgramException("OrderKeys may not be empty.");
		} else if (orders.length == 0) {
			throw new InvalidProgramException("Orders may not be empty");
		} else if (orderKeys.length != orders.length) {
			throw new InvalidProgramException("Number of OrderKeys and Orders must match.");
		}

		if(this.splitGroupKeys != null) {
			throw new InvalidProgramException("DataSource may either be grouped or sorted.");
		}

		this.splitOrdering = new Ordering();

		for(int i=0; i<orderKeys.length; i++) {
			String keyExp = orderKeys[i];
			int[] flatKeys = this.computeFlatKeys(keyExp);

			for(int key : flatKeys) {
				// check for duplicates
				for (int okey : splitOrdering.getFieldPositions()) {
					if (key == okey) {
						throw new InvalidProgramException("Duplicate key in partition key expression " + keyExp);
					}
				}
				// append key
				this.splitOrdering.appendOrdering(key, null, orders[i] );
			}
		}

		return this;
	}

	// --------------------------------------------------------------------------------------------
	
	protected GenericDataSourceBase<OUT, ?> translateToDataFlow() {
		String name = this.name != null ? this.name : "at "+dataSourceLocationName+" ("+inputFormat.getClass().getName()+")";
		if (name.length() > 150) {
			name = name.substring(0, 150);
		}
		
		@SuppressWarnings({ "unchecked", "rawtypes" })
		GenericDataSourceBase<OUT, ?> source = new GenericDataSourceBase(this.inputFormat,
				new OperatorInformation<OUT>(getType()), name);
		source.setDegreeOfParallelism(dop);
		if(this.parameters != null) {
			source.getParameters().addAll(this.parameters);
		}
		source.setSplitProperties(this.splitPartitionKeys, this.splitPartitioner, splitGroupKeys, splitOrdering);
		return source;
	}


	private int[] getAllFlatKeys(String[] fieldExpressions) {

		int[] allKeys = null;

		for(String keyExp : fieldExpressions) {
			int[] flatKeys = this.computeFlatKeys(keyExp);
			if(allKeys == null) {
				allKeys = flatKeys;
			} else {
				// check for duplicates
				for(int key1 : flatKeys) {
					for(int key2 : allKeys) {
						if(key1 == key2) {
							throw new InvalidProgramException("Duplicate key in partition key expression "+keyExp);
						}
					}
				}
				// append flat keys
				int oldLength = allKeys.length;
				int newLength = oldLength + flatKeys.length;
				allKeys = Arrays.copyOf(allKeys, newLength);
				for(int i=0;i<flatKeys.length; i++) {
					allKeys[oldLength+i] = flatKeys[i];
				}
			}
		}

		return allKeys;
	}

	private int[] getAllFlatKeys(int[] fieldPositions) {

		Keys.ExpressionKeys<OUT> ek;
		try {
			ek = new Keys.ExpressionKeys<OUT>(fieldPositions, this.getType());
		} catch(IllegalArgumentException iae) {
			throw new InvalidProgramException("Invalid specification of field expression.", iae);
		}
		return ek.computeLogicalKeyPositions();
	}


	private int[] computeFlatKeys(String fieldExpression) {

		if(this.getType() instanceof CompositeType) {
			// compute flat field positions for (nested) sorting fields
			Keys.ExpressionKeys<OUT> ek;
			try {
				ek = new Keys.ExpressionKeys<OUT>(new String[]{fieldExpression}, this.getType());
			} catch(IllegalArgumentException iae) {
				throw new InvalidProgramException("Invalid specification of field expression.", iae);
			}
			return ek.computeLogicalKeyPositions();
		} else {
			fieldExpression = fieldExpression.trim();
			if (!(fieldExpression.equals("*") || fieldExpression.equals("_"))) {
				throw new InvalidProgramException("Output sorting of non-composite types can only be defined on the full type. " +
						"Use a field wildcard for that (\"*\" or \"_\")");
			} else {
				return new int[]{0};
			}
		}
	}


	public static class SourcePartitionerMarker<T> implements Partitioner<T> {

		String partitionMarker;

		public SourcePartitionerMarker(String partitionMarker) {
			this.partitionMarker = partitionMarker;
		}

		@Override
		public int partition(T key, int numPartitions) {
			throw new UnsupportedOperationException("The DummySourcePartitioner is only used as a marker for compatible partitioning. " +
					"It must not be invoked.");
		}

		@Override
		public boolean equals(Object o) {
			if(o instanceof SourcePartitionerMarker) {
				return this.partitionMarker.equals(((SourcePartitionerMarker) o).partitionMarker);
			} else {
				return false;
			}
		}
	}
}
