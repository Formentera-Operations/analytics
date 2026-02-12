case upper(trim(status_value))
    -- Producing
    when 'PRODUCING' then 'PRODUCING'
    when 'PRODUCER' then 'PRODUCING'
    when 'ACTIVE' then 'PRODUCING'
    when 'ACT' then 'PRODUCING'
    when 'COMPLETING' then 'PRODUCING'
    when 'ESP' then 'PRODUCING'
    when 'ESP - OWNED' then 'PRODUCING'
    when 'FLOWING' then 'PRODUCING'
    when 'FLOWING - CASING' then 'PRODUCING'
    when 'FLOWING - TUBING' then 'PRODUCING'
    when 'GAS LIFT' then 'PRODUCING'

    -- Shut in
    when 'SHUT IN' then 'SHUT_IN'
    when 'SHUT-IN' then 'SHUT_IN'
    when 'SI' then 'SHUT_IN'
    when 'INA' then 'SHUT_IN'
    when 'INACTIVE' then 'SHUT_IN'
    when 'INACTIVE COMPLETED' then 'SHUT_IN'
    when 'INACTIVE INJECTOR' then 'SHUT_IN'
    when 'INACTIVE PRODUCER' then 'SHUT_IN'
    when 'SUSPENDED' then 'SHUT_IN'

    -- Injecting
    when 'INJECTING' then 'INJECTING'
    when 'INJECTOR' then 'INJECTING'
    when 'INJ' then 'INJECTING'

    -- Temp abandoned
    when 'TEMP ABANDONED' then 'TEMP_ABANDONED'
    when 'TA' then 'TEMP_ABANDONED'
    when 'T&A' then 'TEMP_ABANDONED'

    -- Plugged & abandoned
    when 'PLUGGED & ABANDONED' then 'PLUGGED_ABANDONED'
    when 'PLUGGED AND ABANDONED' then 'PLUGGED_ABANDONED'
    when 'P & A' then 'PLUGGED_ABANDONED'
    when 'ABANDONED' then 'PLUGGED_ABANDONED'

    -- Pre-production lifecycle stages
    when 'PLANNED' then 'PLANNED'
    when 'PERMITTED' then 'PERMITTED'
    when 'DUC' then 'DUC'
    when 'DRILLED' then 'DRILLED'
    when 'COMPLETED' then 'COMPLETED'

    -- Other
    when 'SOLD' then 'SOLD'
    when 'PUD' then 'PUD'

    -- Fallback: uppercase with spaces replaced by underscores
    else upper(replace(trim(status_value), ' ', '_'))
end
