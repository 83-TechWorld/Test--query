<select id="getTodoList" resultType="RiskTodo">
  WITH todo_union AS (
    <!-- PRODUCER To-Do -->
    <if test="user.profiles != null and user.profiles.contains('producer')">
      SELECT
        ru.id AS risk_unitary_id,
        rprod.id AS risk_produced_id,
        ru.ref,
        'producer' AS role,
        lw.current_status,
        lw.created_at
      FROM risk_unitary ru
      JOIN risk_produced rprod ON rprod.risk_unitary_id = ru.id
      JOIN (
        SELECT DISTINCT ON (rw.risk_produced_id)
          rw.risk_produced_id,
          rw.current_status,
          rw.created_at
        FROM risk_workflow rw
        ORDER BY rw.risk_produced_id, rw.created_at DESC
      ) lw ON lw.risk_produced_id = rprod.id
      WHERE ru.producer_id = #{user.id}
        AND lw.current_status = 'To_be_accepted'
        AND ru.status IN ('active', 'inactive')
        AND lw.current_status != 'approved'
        <if test="ref != null and ref != ''">
          AND LOWER(ru.ref) LIKE LOWER(CONCAT('%', #{ref}, '%'))
        </if>
    </if>

    <if test="user.profiles.contains('producer') and user.profiles.contains('validator')">
      UNION
    </if>

    <!-- VALIDATOR To-Do -->
    <if test="user.profiles.contains('validator')">
      SELECT
        ru.id AS risk_unitary_id,
        rpv.risk_produced_id,
        ru.ref,
        'validator' AS role,
        lw.current_status,
        lw.created_at
      FROM risk_produced_validators rpv
      JOIN risk_unitary_validator ruv ON rpv.risk_unitary_validator_id = ruv.id
      JOIN risk_produced rprod ON rprod.id = rpv.risk_produced_id
      JOIN risk_unitary ru ON rprod.risk_unitary_id = ru.id
      JOIN (
        SELECT DISTINCT ON (rw.risk_produced_id)
          rw.risk_produced_id,
          rw.current_status,
          rw.created_at
        FROM risk_workflow rw
        ORDER BY rw.risk_produced_id, rw.created_at DESC
      ) lw ON lw.risk_produced_id = rprod.id
      WHERE ruv.person_id = #{user.id}
        AND rpv.status = 'To_be_validated'
        AND lw.current_status = 'To_be_validated'
        AND rpv.id = (
          SELECT MIN(rpv2.id)
          FROM risk_produced_validators rpv2
          WHERE rpv2.risk_produced_id = rpv.risk_produced_id
            AND rpv2.status = 'To_be_validated'
        )
        AND ru.status IN ('active', 'inactive')
        AND lw.current_status != 'approved'
        <if test="ref != null and ref != ''">
          AND LOWER(ru.ref) LIKE LOWER(CONCAT('%', #{ref}, '%'))
        </if>
    </if>
  )

  SELECT DISTINCT ON (risk_produced_id) *
  FROM todo_union
  <if test="sortBy != null and sortOrder != null">
    ORDER BY risk_produced_id, ${sortBy} ${sortOrder}
  </if>
  OFFSET #{offset}
  LIMIT #{limit}
</select>
