with source as (

    select * from {{ source('dlp', 'dim_dlp_platnost') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_cast(datum_aktualizace as date) as dataset_updated_at,
        try_strptime(platnost_od, '%d.%m.%Y')::date as platnost_od,
        try_strptime(platnost_do, '%d.%m.%Y')::date as platnost_do

    from source

)

select * from renamed
