with source as (

    select * from {{ source('dlp', 'dim_dlp_organizace') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_cast(datum_aktualizace as date) as dataset_updated_at,
        zkr_org,
        zem,
        nazev,
        coalesce(vyrobce, '') != '' as is_vyrobce,
        coalesce(drzitel, '') != '' as is_drzitel

    from source

)

select * from renamed
