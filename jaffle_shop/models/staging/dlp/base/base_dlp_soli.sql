with source as (

    select * from {{ source('dlp', 'dim_dlp_soli') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_cast(datum_aktualizace as date) as dataset_updated_at,
        try_cast(kod_latky as integer) as kod_latky,
        try_cast(kod_soli as integer) as kod_soli

    from source

)

select * from renamed
