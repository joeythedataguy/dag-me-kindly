with source as (

    select * from {{ source('dlp', 'dim_dlp_lecivelatky') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_cast(datum_aktualizace as date) as dataset_updated_at,
        try_cast(kod_latky as integer) as kod_latky,
        nazev_inn,
        nazev_en,
        nazev,
        zav

    from source

)

select * from renamed
