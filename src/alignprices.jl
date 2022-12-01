#utility functions for assembling daily data from multiple tickers into aligned price data with options to calculate price changes instead of prices

function alignprices(df::DataFrame; joinmethod=innerjoin, value=:close)
    tickgroups = groupby(sort(select(df, [:ticker, :date, value]), :date), :ticker)
    ticks = [a.ticker for a in keys(tickgroups)]
    mapreduce((a, b) -> joinmethod(a, b, on=:date), ticks) do tick
	    df = tickgroups[(ticker = tick,)]
	    select(df, :date, value => Symbol("$(tick)_$value"))
    end
end

function alignprices(ticklist::Vector; joinmethod=innerjoin, value=:close, getmethod = get_sep)
    df = getmethod(tick=ticklist)
    alignprices(df, joinmethod=joinmethod, value=value)
end

function alignpricechanges(df::DataFrame)
    tickgroups = groupby(sort(select(df, [:ticker, :date, :close]), :date), :ticker)
    ticks = [a.ticker for a in keys(tickgroups)]
    mapreduce((a, b) -> innerjoin(a, b, on=:date), ticks) do tick
	    df = tickgroups[(ticker = tick,)]
        DataFrame((; zip([:date, Symbol("$(tick)_pct_change")], [df.date[2:end], calc_vector_roots(df.close, f = (a, b) -> (a/b)-1)])...))
    end
end

function alignpricechanges(ticklist::Vector; getmethod = get_sep)
    df = getmethod(tick=ticklist)
    alignpricechanges(df)
end

"""
    align_prices_and_changes(ticklist::Vector; getmethod = get_sep)

Returns two DataFrames with prices and percent daily changes for the ticker list.  The first column of each DataFrame is the date while the second column is the value for one of the tickers in the list.  If tickers are normal equities then leave the default getmethod.  Otherwise, if ETFs are being selected change the getmethod to get_sfp.
"""
function align_prices_and_changes(ticklist::Vector; getmethod = get_sep)
    df = getmethod(tick=ticklist)
    prices = alignprices(df)
    pricechanges = alignpricechanges(df)
    (prices, pricechanges)
end

function extractstatistics(pricechanges::DataFrame)
    A = Matrix(dropmissing(select(pricechanges, Not(:date))))
    M = mean(A, dims=1)[:]
    Σ = cov(A)
    (M, Σ)
end

#calculate a function on the elements of v separated by n positions.  By default this will calculate a first difference
function calc_vector_roots(v::AbstractVector{T}; n = 1, f = (a, b) -> a - b) where T 
    l = length(v)
    out = Vector{T}(undef, l - n) 
    @simd for i in 1+n:l
        out[i-n] = f(v[i], v[i-n])
    end
    return out
end