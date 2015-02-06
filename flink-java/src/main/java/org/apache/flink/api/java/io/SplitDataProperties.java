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

package org.apache.flink.api.java.io;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.util.Arrays;

public class SplitDataProperties<T> implements GenericDataSourceBase.SplitDataProperties<T> {

	private TypeInformation<T> type;

	private int[] splitPartitionKeys;

	private Partitioner<T> splitPartitioner;

	private int[] splitGroupKeys;

	private Ordering splitOrdering;

	private boolean oneSplitPerTask = false;

	public SplitDataProperties(TypeInformation<T> type) {
		this.type = type;
	}

	/**
	 * Defines that data is partitioned across input splits on the fields defined by field positions.
	 * All records sharing the same key (combination) must be contained in a single input split.
	 * <br>
	 * <b>
	 *     IMPORTANT: Providing wrong information with SplitDataProperties can cause wrong results!
	 * </b>
	 *
	 * @param partitionKeys The field positions of the partitioning keys.
	 * @result This SplitDataProperties object.
	 */
	public SplitDataProperties<T> splitsPartitionedBy(int... partitionKeys) {
		return this.splitsPartitionedBy(null, partitionKeys);
	}

	/**
	 * Defines that data is partitioned using an identifiable method
	 * across input splits on the fields defined by field positions.
	 * All records sharing the same key (combination) must be contained in a single input split.
	 * <br>
	 * <b>
	 *     IMPORTANT: Providing wrong information with SplitDataProperties can cause wrong results!
	 * </b>
	 *
	 * @param partitionMethodId An ID for the method that was used to partition the data across splits.
	 * @param partitionKeys The field positions of the partitioning keys.
	 * @result This SplitDataProperties object.
	 */
	public SplitDataProperties<T> splitsPartitionedBy(String partitionMethodId, int... partitionKeys) {

		if (partitionKeys == null) {
			throw new InvalidProgramException("PartitionKeys may not be null.");
		} else if (partitionKeys.length == 0) {
			throw new InvalidProgramException("PartitionKeys may not be empty.");
		}

		if (!(this.type instanceof TupleTypeInfo)) {
			throw new InvalidProgramException("Field position keys may only be defined for tuple data types");
		}

		this.splitPartitionKeys = getAllFlatKeys(partitionKeys);
		if (partitionMethodId != null) {
			this.splitPartitioner = new SourcePartitionerMarker<T>(partitionMethodId);
		} else {
			this.splitPartitioner = null;
		}

		return this;
	}

	/**
	 * Defines that data is partitioned across input splits on the fields defined by field expressions.
	 * All records sharing the same key (combination) must be contained in a single input split.
	 * <br>
	 * <b>
	 *     IMPORTANT: Providing wrong information with SplitDataProperties can cause wrong results!
	 * </b>
	 *
	 * @param partitionKeys The field expressions of the partitioning keys.
	 * @result This SplitDataProperties object.
	 */
	public SplitDataProperties<T> splitsPartitionedBy(String partitionKeys) {
		return this.splitsPartitionedBy(null, partitionKeys);
	}

	/**
	 * Defines that data is partitioned using an identifiable method
	 * across input splits on the fields defined by field expressions.
	 * All records sharing the same key (combination) must be contained in a single input split.
	 * <br>
	 * <b>
	 *     IMPORTANT: Providing wrong information with SplitDataProperties can cause wrong results!
	 * </b>
	 *
	 * @param partitionMethodId An ID for the method that was used to partition the data across splits.
	 * @param partitionKeys The field expressions of the partitioning keys.
	 * @result This SplitDataProperties object.
	 */
	public SplitDataProperties<T> splitsPartitionedBy(String partitionMethodId, String partitionKeys) {

		if(partitionKeys == null) {
			throw new InvalidProgramException("PartitionKeys may not be null.");
		}

		String[] partitionKeysA = partitionKeys.split(";");
		if (partitionKeysA.length == 0) {
			throw new InvalidProgramException("PartitionKeys may not be empty.");
		}

		this.splitPartitionKeys = getAllFlatKeys(partitionKeysA);
		if(partitionMethodId != null) {
			this.splitPartitioner = new SourcePartitionerMarker<T>(partitionMethodId);
		}
		else {
			this.splitPartitioner = null;
		}

		return this;
	}

	/**
	 * Defines that the data within an input split is grouped on the fields defined by the field positions.
	 * All records sharing the same key (combination) must be subsequently emitted by the input
	 * format for each input split.
	 * <br>
	 * <b>
	 *     IMPORTANT: Providing wrong information with SplitDataProperties can cause wrong results!
	 * </b>
	 *
	 * @param groupKeys The field positions of the grouping keys.
	 * @result This SplitDataProperties object.
	 */
	public SplitDataProperties<T> splitsGroupedBy(int... groupKeys) {

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
	 * Defines that the data within an input split is grouped on the fields defined by the field expressions.
	 * All records sharing the same key (combination) must be subsequently emitted by the input
	 * format for each input split.
	 * <br>
	 * <b>
	 *     IMPORTANT: Providing wrong information with SplitDataProperties can cause wrong results!
	 * </b>
	 *
	 * @param groupKeys The field expressions of the grouping keys.
	 * @result This SplitDataProperties object.
	 */
	public SplitDataProperties<T> splitsGroupedBy(String groupKeys) {

		if(groupKeys == null) {
			throw new InvalidProgramException("GroupKeys may not be null.");
		}

		String[] groupKeysA = groupKeys.split(";");
		if (groupKeysA.length == 0) {
			throw new InvalidProgramException("GroupKeys may not be empty.");
		}

		if(this.splitOrdering != null) {
			throw new InvalidProgramException("DataSource may either be grouped or sorted.");
		}

		this.splitGroupKeys = getAllFlatKeys(groupKeysA);

		return this;
	}

	/**
	 * Defines that the data within an input split is sorted on the fields defined by the field positions
	 * in the specified orders.
	 * All records of an input split must be emitted by the input format in the defined order.
	 * <br>
	 * <b>
	 *     IMPORTANT: Providing wrong information with SplitDataProperties can cause wrong results!
	 * </b>
	 *
	 * @param orderKeys The field positions of the grouping keys.
	 * @param orders The orders of the fields.
	 * @result This SplitDataProperties object.
	 */
	public SplitDataProperties<T> splitsOrderedBy(int[] orderKeys, Order[] orders) {

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
	 * Defines that the data within an input split is sorted on the fields defined by the field expressions
	 * in the specified orders.
	 * All records of an input split must be emitted by the input format in the defined order.
	 * <br>
	 * <b>
	 *     IMPORTANT: Providing wrong information with SplitDataProperties can cause wrong results!
	 * </b>
	 *
	 * @param orderKeys The field expressions of the grouping keys.
	 * @param orders The orders of the fields.
	 * @result This SplitDataProperties object.
	 */
	public SplitDataProperties<T> splitsOrderedBy(String orderKeys, Order[] orders) {

		if(orderKeys == null || orders == null) {
			throw new InvalidProgramException("OrderKeys or Orders may not be null.");
		}

		String[] orderKeysA = orderKeys.split(";");
		if (orderKeysA.length == 0) {
			throw new InvalidProgramException("OrderKeys may not be empty.");
		} else if (orders.length == 0) {
			throw new InvalidProgramException("Orders may not be empty");
		} else if (orderKeysA.length != orders.length) {
			throw new InvalidProgramException("Number of OrderKeys and Orders must match.");
		}

		if(this.splitGroupKeys != null) {
			throw new InvalidProgramException("DataSource may either be grouped or sorted.");
		}

		this.splitOrdering = new Ordering();

		for(int i=0; i<orderKeysA.length; i++) {
			String keyExp = orderKeysA[i];
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

	/**
	 * Defines that each parallel task receives at most one input split.
	 * <br>
	 * <b>
	 *     IMPORTANT: Setting this property changes the data properties of the data source
	 *     and can cause wrong results if this property is violated.
	 * </b>
	 *
	 */
	public void setOnlyOneSplitPerTask() {
		this.oneSplitPerTask = true;
	}


	public int[] getSplitPartitionKeys() {
		return this.splitPartitionKeys;
	}

	public Partitioner<T> getSplitPartitioner() {
		return this.splitPartitioner;
	}

	public int[] getSplitGroupKeys() {
		return this.splitGroupKeys;
	}

	public Ordering getSplitOrder() {
		return this.splitOrdering;
	}

	public boolean onlyOneSplitPerTask() {
		return this.oneSplitPerTask;
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

		Keys.ExpressionKeys<T> ek;
		try {
			ek = new Keys.ExpressionKeys<T>(fieldPositions, this.type);
		} catch(IllegalArgumentException iae) {
			throw new InvalidProgramException("Invalid specification of field expression.", iae);
		}
		return ek.computeLogicalKeyPositions();
	}


	private int[] computeFlatKeys(String fieldExpression) {

		if(this.type instanceof CompositeType) {
			// compute flat field positions for (nested) sorting fields
			Keys.ExpressionKeys<T> ek;
			try {
				ek = new Keys.ExpressionKeys<T>(new String[]{fieldExpression}, this.type);
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
			throw new UnsupportedOperationException("The SourcePartitionerMarker is only used as a marker for compatible partitioning. " +
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
