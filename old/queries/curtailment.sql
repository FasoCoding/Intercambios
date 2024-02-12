SELECT t_child.name AS child_name, t_period_0.datetime, t_period_0.day_id, t_period_0.period_of_day, SUM(d.value) AS curtailment
FROM ((((((((((t_data_0 AS d 
    INNER JOIN t_key AS k ON d.key_id = k.key_id) 
    INNER JOIN t_period_0 ON d.period_id = t_period_0.interval_id) 
    INNER JOIN t_membership AS m ON k.membership_id = m.membership_id) 
    INNER JOIN t_property AS p ON k.property_id = p.property_id) 
    INNER JOIN t_unit ON p.unit_id = t_unit.unit_id) 
    INNER JOIN t_collection AS c ON m.collection_id = c.collection_id) 
    INNER JOIN t_object AS t_parent ON m.parent_object_id = t_parent.object_id) 
    INNER JOIN t_object AS t_child ON m.child_object_id = t_child.object_id) 
    INNER JOIN t_category ON (t_child.class_id = t_category.class_id) AND (t_child.category_id = t_category.category_id)) 
    INNER JOIN t_model ON k.model_id = t_model.model_id) 
    INNER JOIN t_class AS tc ON t_child.class_id = tc.class_id
WHERE (p.name = 'Capacity Curtailed') And (tc.name = 'Generator')
GROUP BY t_child.name,  t_period_0.datetime, t_period_0.day_id, t_period_0.period_of_day;