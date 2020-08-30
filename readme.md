<h2>Multiphase merge sort</h2>

<h3>How to build:</h3>

<ul>
<li>clone the repo</li>
<li><b>cd</b> into the root of the repo</li>
<li>run <b>build.cmd</b></li>
<li>the <b>bin</b> directory will appear in the repository root</li>
</ul>

<h3>How to run e2e</h3>

<ul>
<li>go to bin/e2e</li>
<li>run e2e.exe <i>"full-path-to-directory-for-tmp-files"</i></li>
<li>e2e will generate the random file with duplicate strings, sort it, and validate</li>
</ul>

<h3>How to run generator</h3>

<ul>
<li>go to bin/generator</li>
<li>run generator.exe <i>"full-path-to-generated-file"</i><i>&lt;size of the file in GB&gt;</i></li>
<li>the generator will create a random file of the given size at the specified path</li>
</ul>

<h3>How to run sorter</h3>

<ul>
<li>generate the input file using generator</li>
<li>go to bin/sorter</li>
<li>run sorter.exe <i>"full-path-to-generated-file"</i></li>
<li>the sorter will perform sorting and write the sorted contents back to the input file</li>
</ul>