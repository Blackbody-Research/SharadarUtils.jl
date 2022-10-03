module SharadarUtils

using HTTP, ZipFile, Dates, CSV, DataFrames

global key = ""

const urlprefix = "https://data.nasdaq.com/api/v3/datatables/SHARADAR/"
const exportflag = "qopts.export=true"

function setkey(newkey::AbstractString)
    global key = newkey
end


#URL formation functions
#returns proper url suffix with api key on demand
urlsuffix() = "api_key=$key"
maketickurl(tick::AbstractString) = "ticker=$tick"
maketickurl(::Missing) = missing
maketickurl(ticks::Vector{T}) where T <: AbstractString = string("ticker=", reduce((a, b) -> "$a,$b", ticks))
makedateurl(date::Date) = "date=$date"
makedateurl(::Missing) = missing
makedaterangeurl(date1::Date, date2::Date) = "date.gte=$date1&date.lte=$date2" 
makedaterangeurl(::Missing, ::Missing) = missing 
makedateupdateurl(date::Date) = "lastupdated.gte=$date" 
makedateupdateurl(::Missing) = missing 

formurlprefix(table::AbstractString, dataexport::Bool) = reduceurls("$urlprefix$table.csv?", dataexport ? exportflag : missing) 

reduceurls(a::AbstractString, b::AbstractString) = string(a, "&", b)
reduceurls(::Missing, b::AbstractString) = b
reduceurls(a::AbstractString, ::Missing) = a
reduceurls(::Missing, ::Missing) = missing
reduceurls(urls...) = reduce(reduceurls, urls)
reduceurls(urls::Vector{T}) where T <: AbstractString = reduce(reduceurls, urls)

makedimurl(dimension::AbstractString) = "dimension=$dimension"
makedimurl(::Missing) = missing

"""
    get_metadata(table="SF1"; tick = missing, tabexport=true)

Returns a DataFrame with the meta data table.  Options to select meta data for a specific ticker/list of tickers.  Default behavior is to retrive the entire metadata table for the SF1 database as a file export to ensure all data is included.  
"""
function get_metadata(table="SF1"; tick = missing, tabexport=true)
    url = reduceurls(formurlprefix("TICKERS", tabexport), "table=$table", maketickurl(tick), urlsuffix())
    get_table(url, tabexport)
end


"""
    get_daily_table(tab="DAILY"; tick=missing, date=missing, daterange=(missing, missing), updatedate=missing, tabexport=true)

Returns a DataFrame of the daily table.  Options to select a specific ticker/list of tickers, date, daterange, and only updates past a certain date.  Default behavior is to retrive the entire daily table as a file export to ensure all data is included.  
"""
function get_daily_table(tab="DAILY"; tick=missing, date=missing, daterange=(missing, missing), updatedate=missing, tabexport=true)
    url = reduceurls(formurlprefix(tab, tabexport), maketickurl(tick), makedateurl(date), makedaterangeurl(daterange...), makedateupdateurl(updatedate), urlsuffix())
    get_table(url, tabexport)
end


"""
    get_sep(;tick=missing, date=missing, daterange=(missing, missing), updatedate=missing, tabexport=true)

Returns a DataFrame with the SEP price table.  Options to return a table for a single/list of tickers, a single date, a date range, or data updated since a given date.  Default behavior is to retrieve the entire SEP table as a file export to ensure all data is included.

"""

get_sep(;tick=missing, date=missing, daterange=(missing, missing), updatedate=missing, tabexport=true) = get_daily_table("SEP", tick=tick, daterange=daterange, updatedate=updatedate, tabexport=tabexport)

"""
    get_fundamentals(;dimension=missing, tick=missing, tabexport=true)

Returns a DataFrame with the SF1 table fundamentals.  Options to return a table for a single dimension or ticker/list of tickers.  Default behavior is to retrieve the entire SF1 table as a file export to ensure all data is included.
"""
function get_fundamentals(;dimension=missing, tick=missing, tabexport=true)
    url = reduceurls(formurlprefix("SF1", tabexport), maketickurl(tick), makedimurl(dimension), urlsuffix())
    get_table(url, tabexport)
end
"""
    get_export_table(url::AbstractString; missingstring=[""], buffer_in_memory=false)

Returns a DataFrame for a sharadar export request in which a file is generated on demand in a zip format.  The function must scan the url until the file is ready and then download it either onto disk or into memory in preparation for it ot be unizipped and read into a DataFrame.  Optionally pass a vector of missing strings and select whether to read into memory or use a temporary file by default.  The purpose of the export table is to retrieve data with more than 10,000 rows.
"""
function get_export_table(url::AbstractString, missingstring, buffer_in_memory::Bool)
    output = get_table(url, false)
    status = output[1, "file.status"]
    link = output[1, "file.link"]
    while status != "fresh"
        output = get_table(url, false)
        status = output[1, "file.status"]
        link = output[1, "file.link"]
        sleep(0.10)
    end
    CSV.read(ZipFile.Reader(IOBuffer(HTTP.get(link).body)).files[1], DataFrame, missingstring=missingstring, buffer_in_memory=buffer_in_memory)
end


#return a table from a non exported sharadar api call
function get_table(url::AbstractString, missingstring, buffer_in_memory::Bool)
    r = HTTP.get(url)
    CSV.read(r.body, DataFrame)
end

function get_table(url::AbstractString, tabexport::Bool; missingstring=["", "NA", "N/A"], buffer_in_memory=true)
    tabexport && return get_export_table(url, missingstring, buffer_in_memory)
    return get_table(url, missingstring, buffer_in_memory)
end


export setkey, get_metadata
end # module SharadarUtils
