with dopinglp as (

    select * from {{ ref('base_dlp_dopinglp') }}

),

doping as (

    select doping, nazev as doping_nazev from {{ ref('base_dlp_doping') }}

)

select
    dopinglp.kod_sukl,
    dopinglp.kod_doping,
    doping.doping_nazev

from dopinglp
left join doping on dopinglp.kod_doping = doping.doping
