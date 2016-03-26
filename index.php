<?php //echo $_GET['typeahead'];
require 'connection.php';
$query = 0;
if(isset($_GET['typeahead']))
{
    $key = $_GET['typeahead'];
    $array = array();

    $query = $db->query("select * from user where name LIKE '{$key}%'");

}
?>
<html>
  <head>
    <title>Ajax Search Box using PHP and MySQL</title>
    <script src="http://ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js"></script>
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap.min.css">
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/css/bootstrap-theme.min.css">
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/bootstrap.min.js"></script>
    <script src="typeahead.min.js"></script>
    <script>
    $(document).ready(function(){
    $('input.typeahead').typeahead({
        name: 'typeahead',
        remote:'search.php?key=%QUERY',
        limit : 10
    });
});
    </script>
    <style type="text/css">
.bs-example{
	font-family: sans-serif;
	position: relative;
	margin: 50px;
}
.typeahead, .tt-query, .tt-hint {
	border: 2px solid #CCCCCC;
	border-radius: 8px;
	font-size: 24px;
	height: 30px;
	line-height: 30px;
	outline: medium none;
	padding: 8px 12px;
	width: 396px;
}
.typeahead {
	background-color: #FFFFFF;
}
.typeahead:focus {
	border: 2px solid #0097CF;
}
.tt-query {
	box-shadow: 0 1px 1px rgba(0, 0, 0, 0.075) inset;
}
.tt-hint {
	color: #999999;
}
.tt-dropdown-menu {
	background-color: #FFFFFF;
	border: 1px solid rgba(0, 0, 0, 0.2);
	border-radius: 8px;
	box-shadow: 0 5px 10px rgba(0, 0, 0, 0.2);
	margin-top: 12px;
	padding: 8px 0;
	width: 422px;
}
.tt-suggestion {
	font-size: 24px;
	line-height: 24px;
	padding: 3px 20px;
}
.tt-suggestion.tt-is-under-cursor {
	background-color: #0097CF;
	color: #FFFFFF;
}
.tt-suggestion p {
	margin: 0;
}
</style>
  </head>
  <body>
    <div class="container">
        <div class="row">
            <h1>My website</h1>
            <form action="" method="get">
            <div class="col-md-6 col-md-offset-3">
            <input type="text" name="typeahead" class="typeahead tt-query" autocomplete="off" spellcheck="false" placeholder="<?php if(!isset($_GET['typeahead'])) echo '';else echo $key; ?>">
            </div>
            <div class="col-md-2">
               <button type="submit" name="submit">Search</button>
            </div>
            </form>
        </div>
        <br>
        <div>
        <?php
                if((int)$query == false) {
                    echo 'No result matched';
                }
                else
                {
                    echo '<table class="table table-bordered"><tr><th>Name</th><th>Phone</th><th>Address</th><th>Age</th></tr>';

                       foreach($query as $row)
                       {
                            echo '<tr><td>'.$row['name'].'</td><td>'.$row['phone'].'</td><td>'.$row['address'].'</td><td>'.$row['age'].'</td></tr>';

                       }
                    echo '</table>';
                }
        ?>
        </div>
    </div>
  </body>
</html>
