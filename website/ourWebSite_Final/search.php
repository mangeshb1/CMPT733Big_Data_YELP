<?php
    include ('connection.php');
    $key=$_GET['key'];
    $array = array();

    $query = $db->query("select * from restaurant where name LIKE '%{$key}%'");
    while($row = $query->fetch_assoc())
    {
      $array[] = $row['name'];
    }
    echo json_encode($array);
?>
