case upper(trim(method_value))
    -- Rod pump variants
    when 'ROD PUMP' then 'ROD_PUMP'
    when 'PUMPING' then 'ROD_PUMP'

    -- Gas lift variants
    when 'GAS LIFT' then 'GAS_LIFT'
    when 'PLUNGER ASSISTED GAS LIFT' then 'PLUNGER_GAS_LIFT'
    when 'PLUNGER LIFT - CONV' then 'PLUNGER_LIFT'

    -- Other artificial lift
    when 'ESP' then 'ESP'
    when 'JET PUMP' then 'JET_PUMP'
    when 'PCP' then 'PCP'
    when 'BMEG' then 'BMEG'
    when 'INTERMITTENT' then 'INTERMITTENT'

    -- Flowing (no artificial lift)
    when 'FLOWING' then 'FLOWING'

    -- Injection / disposal
    when 'INJECTION' then 'INJECTION'
    when 'SWD' then 'SWD'

    -- Status values that are not lift methods — return null
    when 'PLUGGED & ABANDONED' then null
    when 'TEMP ABANDONED' then null
    when 'SHUT IN' then null
    when 'UNKNOWN' then null

    -- Non-operated (not a lift method but appears in source data)
    when 'NON-OP' then 'NON_OPERATED'

    else upper(trim(method_value))
end
