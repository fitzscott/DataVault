param($epc)

gci *_${epc}.txt | ForEach-Object {
   $nm = $_.Name
   $nunm = "uniq_$nm"
   Get-Content $_ | Sort-Object -Unique | Out-File -Encoding ascii ${nunm}
   Remove-Item ${nm}
   Rename-Item ${nunm} ${nm}
}
