--http://lua-users.org/wiki/SimpleStats
-- Table to hold statistical functions
stats={}

-- Get the mean value of a table
function stats.mean( t )
  local sum = 0
  local count= 0

  for k,v in pairs(t) do
    --if type(v) == 'number' then
      sum = sum + v
      count = count + 1
   -- end
  end

  return (sum / count)
end

-- Get the standard deviation of a table
function stats.standardDeviation( t )
  local m
  local vm
  local sum = 0
  local count = 0
  local result

  m = stats.mean( t )

  for k,v in pairs(t) do
   -- if type(v) == 'number' then
      vm = v - m
      sum = sum + (vm * vm)
      count = count + 1
   -- end
  end

  result = math.sqrt(sum / (count-1))

  return result
end

filename=arg[1]

local lines = {}
local lines_per_cycle={}
-- read the lines in table 'lines'
if filename~=nil then
	for line in io.lines(filename) do 
		table.insert(lines,tonumber(line))
	end
else
	for line in io.lines() do 
		table.insert(lines,tonumber(line))
	end
end

mean=stats.mean(lines)
stdev=stats.standardDeviation(lines)
print(mean,stdev)
