with stg_lek13 as (

    select * from {{ ref('stg_lek13') }}

)

select
    *,
    round(
        100.0 * (rank() over (order by pocet_baleni) - 1) / count(*) over (),
        2
    ) as percentil_pocet_baleni

from stg_lek13
