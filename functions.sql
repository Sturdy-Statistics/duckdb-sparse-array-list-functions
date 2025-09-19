-- Extract value from sparse list representation at given position
-- Parameters: position (index to extract), inds (indices list), vals (values list)
-- Returns 0 if position not found in indices
CREATE OR REPLACE TEMPORARY MACRO sparse_list_extract(position, inds, vals) AS  
    COALESCE(vals[list_position(inds, position)], 0);

-- Select multiple values from sparse list at given positions
-- Parameters: positions (list of indices), inds (indices list), vals (values list)
-- Returns list of values corresponding to the positions
CREATE OR REPLACE TEMPORARY MACRO sparse_list_select(positions, inds, vals) AS  
    list_transform(positions, x-> sparse_list_extract(x, inds, vals));

-- Compute dot product between dense list and sparse list
-- Parameters: arr (dense list), inds (sparse indices), vals (sparse values)
-- Selects elements from dense list at sparse indices and computes dot product
CREATE OR REPLACE TEMPORARY MACRO dense_x_sparse_dot_product(arr, inds, vals) AS 
    list_dot_product(list_select(arr, inds)::DOUBLE[], vals::DOUBLE[]);

-- Convert sparse list representation to dense list
-- Parameters: K (size of dense list), inds (sparse indices), vals (sparse values)
-- Creates dense list of size K with values from sparse representation, 0 elsewhere
CREATE OR REPLACE TEMPORARY MACRO sparse_to_dense(K, inds, vals) AS 
    list_transform(
        generate_series(1, K), 
        x-> coalesce(list_extract(vals, list_position(inds, x)), 0)
    );

-- Aggregate arrays by element-wise summation
-- Parameters: arrs (list of arrays to sum)
-- Reduces list of arrays to single list by summing corresponding elements
CREATE OR REPLACE TEMPORARY MACRO agg_array_sum(arrs) AS 
    list_reduce(
        arrs, 
        (acc, y) -> list_transform(
            list_zip(acc, y),
            z -> z[1] + z[2] 
        )
    );
