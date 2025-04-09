<select id="getTodoList" resultType="RiskTodo">
  SELECT * FROM (
    SELECT DISTINCT ON (risk_produced_id)
      *,
      COUNT(*) OVER () AS full_count
    FROM (
      <!-- Producer To-Do -->
      <if test="user.profiles != null and user.profiles.contains('producer')">
        SELECT
          ru.id AS risk_unitary_id,
          rprod.id AS risk_produced_id,
          ru.ref,
          'producer' AS role,
          rprod.status AS current_status,
          rprod.created_at
        FROM risk_unitary ru
        JOIN risk_produced rprod ON rprod.risk_unitary_id = ru.id
        WHERE (ru.producer_id = #{user.id} OR ru.back_up_producer_id = #{user.id})
          AND rprod.status = 'To_be_produced'
          AND ru.status IN ('active', 'inactive')
          <if test="ref != null and ref != ''">
            AND LOWER(ru.ref) LIKE LOWER(CONCAT('%', #{ref}, '%'))
          </if>
      </if>

      <if test="user.profiles.contains('producer') and user.profiles.contains('validator')">
        UNION ALL
      </if>

      <!-- Validator To-Do -->
      <if test="user.profiles != null and user.profiles.contains('validator')">
        SELECT
          ru.id AS risk_unitary_id,
          rprod.id AS risk_produced_id,
          ru.ref,
          'validator' AS role,
          rprod.status AS current_status,
          rprod.created_at
        FROM risk_produced_validators rpv
        JOIN risk_unitary_validator ruv ON rpv.risk_unitary_validator_id = ruv.id
        JOIN risk_produced rprod ON rprod.id = rpv.risk_produced_id
        JOIN risk_unitary ru ON rprod.risk_unitary_id = ru.id
        WHERE (ruv.person_id = #{user.id} OR ruv.back_up_person_id = #{user.id})
          AND rprod.status = 'To_be_validated'
          AND rpv.status = 'To_be_validated'
          AND rpv.id = (
            SELECT MIN(rpv2.id)
            FROM risk_produced_validators rpv2
            WHERE rpv2.risk_produced_id = rpv.risk_produced_id
              AND rpv2.status = 'To_be_validated'
          )
          AND ru.status IN ('active', 'inactive')
          <if test="ref != null and ref != ''">
            AND LOWER(ru.ref) LIKE LOWER(CONCAT('%', #{ref}, '%'))
          </if>
      </if>
    ) merged_todos
    ORDER BY risk_produced_id, role  -- prioritizes "producer" over "validator"
  ) final
  <if test="sortBy != null and sortOrder != null">
    ORDER BY ${sortBy} ${sortOrder}
  </if>
  OFFSET #{offset}
  LIMIT #{limit}
</select>
