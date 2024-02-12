SELECT 
    t_child.name AS generator,
    t_property.name AS property,
    t_period_0.datetime,
    t_data_0.key_id AS data_key,
    t_data_0.period_id AS data_period,
    t_data_0.value
FROM ((((((((t_membership
INNER JOIN t_collection ON t_membership.collection_id = t_collection.collection_id)
INNER JOIN t_object AS t_parent ON t_membership.parent_object_id = t_parent.object_id)
INNER JOIN t_object AS t_child ON t_membership.child_object_id = t_child.object_id)
INNER JOIN t_property ON t_collection.collection_id = t_property.collection_id)
INNER JOIN t_key ON t_membership.membership_id = t_key.membership_id AND t_property.property_id = t_key.property_id)
INNER JOIN t_data_0 ON t_key.key_id = t_data_0.key_id)
INNER JOIN t_phase_3 ON t_data_0.period_id = t_phase_3.period_id)
INNER JOIN t_period_0 ON t_phase_3.interval_id = t_period_0.interval_id)
INNER JOIN t_category ON t_child.category_id = t_category.category_id
WHERE t_collection.collection_id = 1 AND t_property.property_id IN (1, 28) AND t_category.category_id IN (95, 96, 99, 100)