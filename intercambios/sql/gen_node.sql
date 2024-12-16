SELECT 
    gen_obj.name AS generator, 
    node_obj.name AS node
FROM ((t_membership
INNER JOIN t_object as node_obj ON t_membership.child_object_id = node_obj.object_id)
INNER JOIN t_object as gen_obj ON t_membership.parent_object_id = gen_obj.object_id)
WHERE node_obj.class_id = 22 AND gen_obj.class_id = 2 AND gen_obj.category_id IN (98, 99, 102, 103) AND t_membership.collection_id = 12;