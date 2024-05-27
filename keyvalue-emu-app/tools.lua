local function ord(c)
    return string.format('%02X', string.byte(c))
end

function Bin2hex(str)
    return str:gsub('.', ord)
end

function SplitString(delim, str)
    local list = {}
    local start = 1
    local delimPos, delimEndPos = string.find( str, delim, start )

    while delimPos do
        table.insert( list, string.sub( str, start, delimPos-1 ) )
        start = delimEndPos + 1
        delimPos, delimEndPos = string.find( str, delim, start )
    end

    table.insert( list, string.sub( str, start ) )
    return list
end
