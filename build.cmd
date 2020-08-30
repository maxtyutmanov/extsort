cd %~dp0
dotnet publish %CD%\src\ExtSort\ExtSort.Sorter\ExtSort.Sorter.csproj -c Release --output %CD%\bin\sorter
dotnet publish %CD%\src\ExtSort\ExtSort.Generator\ExtSort.Generator.csproj -c Release --output %CD%\bin\generator
dotnet publish %CD%\src\ExtSort\ExtSort.EndToEndTest\ExtSort.EndToEndTest.csproj -c Release --output %CD%\bin\e2e