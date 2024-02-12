SELECT DISTINCT t_child.name AS child_name, t_object_1.name AS Node
FROM (t_object AS t_object_1
    INNER JOIN t_class AS tc2 ON t_object_1.class_id = tc2.class_id)
    INNER JOIN (t_membership
    INNER JOIN ((((t_data_0 AS d INNER JOIN t_key AS k
        ON d.key_id = k.key_id)
    INNER JOIN t_membership AS m
        ON k.membership_id = m.membership_id)
    INNER JOIN t_property AS p ON k.property_id = p.property_id)
    INNER JOIN ((t_object AS t_child
    INNER JOIN t_class AS tc ON t_child.class_id = tc.class_id)
    INNER JOIN t_category
        ON (t_child.category_id = t_category.category_id)
        AND (t_child.class_id = t_category.class_id))
        ON m.child_object_id = t_child.object_id)
        ON t_membership.parent_object_id = t_child.object_id)
        ON t_object_1.object_id = t_membership.child_object_id
GROUP BY t_child.object_id, t_child.name, d.value,t_object_1.name, t_object_1.object_id, tc2.name,p.name, tc.name, t_category.name, t_child.name
HAVING (((tc2.name)='Node')
    AND ((p.name)='SRMC')
    AND ((tc.name)='Generator')
    AND ((t_category.name)<>'Termicas Ficticias'
    And (t_category.name)<>'Hydro Gen Group A'
    And (t_category.name)<>'Hydro Gen Group B'
    And (t_category.name)<>'Hydro Gen Group C'
    And (t_category.name)<>'Thermal Gen N. Zone'
    And (t_category.name)<>'Thermal Gen S. Zone'));