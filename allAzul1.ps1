param($cnt)

echo "Starting cycled agent evaluation & Snowflake refresh"

for ($i = 0; $i -lt ${cnt}; $i++) {
echo "Starting set ${i}"
Get-Date

echo "Kicking off 10"
$av3 = Start-Job -FilePath C:\Users\Fitzs\PycharmProjects\Azul\tw10.ps1
Start-Sleep 180

echo "Kicking off 9"
$av2 = Start-Job -FilePath C:\Users\Fitzs\PycharmProjects\Azul\tw9.ps1
Start-Sleep 180

echo "Kicking off 1"
$av1 = Start-Job -FilePath C:\Users\Fitzs\PycharmProjects\Azul\tw1.ps1

Wait-Job $av1
Receive-Job $av1
Get-Date

Wait-Job $av2
Receive-Job $av2
Get-Date

Wait-Job $av3
Receive-Job $av3

$dt = Get-Date -UFormat "%Y%m%d%H%M"
echo "Starting Snowflake fact load"
Get-Date
C:\Users\Fitzs\PycharmProjects\DataVault\venv\Scripts\python.exe C:/Users/Fitzs/PycharmProjects/DataVault/ExtractMySQLTbl.py 2>&1 | Tee-Object etlMySQL2Snowflake_${dt}.txt
echo "Snowflake fact load complete"
Get-Date

}

echo "Completed cycled agent evaluation & Snowflake refresh"

