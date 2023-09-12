const { MongoClient } = require('mongodb');
const express = require('express');

const app = express();
const port = 4000; // Puerto en el que escuchará el servidor Express

// URL de conexión a MongoDB con el puerto 4000
const mongoURL = 'mongodb+srv://edwingstiven2023:1234@clusterfarmacia.ehqgtfk.mongodb.net/';
const client = new MongoClient(mongoURL);

// Nombre de la base de datos
const dbName = 'farmaciaCampus';

// Función para conectar a la base de datos
async function conectarDB() {
  try {
    await client.connect();
    console.log('Conexión exitosa al servidor de MongoDB');
  } catch (err) {
    console.error('Error en MongoDB:', err);
  }
}

// Función para cerrar la conexión a la base de datos
async function cerrarDB() {
  try {
    await client.close();
    console.log('Conexión cerrada al servidor de MongoDB');
  } catch (err) {
    console.error('Error al cerrar la conexión de MongoDB:', err);
  }
}

// Ruta para obtener todos los medicamentos con menos de 50 unidades en stock
app.get('/medicamentos-bajos-en-stock', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const collection = db.collection('Medicamentos');

    const medicamentosBajosEnStock = await collection.find({ "stock": { $lt: 50 } }).toArray();

    res.json(medicamentosBajosEnStock);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

// Ruta para listar los proveedores con su información de contacto en medicamentos
app.get('/proveedores-de-medicamentos', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const collection = db.collection('Medicamentos');

    const proveedoresDeMedicamentos = await collection.distinct("proveedor", {});

    res.json(proveedoresDeMedicamentos);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

// Ruta para obtener medicamentos comprados al 'Proveedor A'
app.get('/medicamentos-del-proveedor-a', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const collection = db.collection('Compras');

    const medicamentosDelProveedorA = await collection.find({ "proveedor.nombre": "ProveedorA" }).toArray();

    res.json(medicamentosDelProveedorA);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

// Ruta para obtener recetas médicas emitidas después del 1 de enero de 2023
app.get('/recetas-despues-de-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const collection = db.collection('Recetas'); // Asume que tienes una colección de Recetas

    const recetasDespuesDe2023 = await collection.find({ "fechaEmision": { $gt: new Date("2023-01-01") } }).toArray();

    res.json(recetasDespuesDe2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

// Ruta para obtener el total de ventas del medicamento 'Paracetamol'
app.get('/total-ventas-paracetamol', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');
    const medicamentosCollection = db.collection('Medicamentos');

    const paracetamol = await medicamentosCollection.findOne({ "nombre": "Paracetamol" });
    const totalVentasParacetamol = await ventasCollection.aggregate([
      { $match: { "medicamentosVendidos.nombreMedicamento": "Paracetamol" } },
      { $group: { _id: null, total: { $sum: "$medicamentosVendidos.cantidadVendida" } } }
    ]).toArray();

    res.json(totalVentasParacetamol);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

// Ruta para obtener medicamentos que caducan antes del 1 de enero de 2024
app.get('/medicamentos-caducan-antes-2024', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const collection = db.collection('Medicamentos');

    const medicamentosCaducanAntes2024 = await collection.find({ "fechaCaducidad": { $lt: new Date("2024-01-01") } }).toArray();

    res.json(medicamentosCaducanAntes2024);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

// Ruta para obtener el total de medicamentos vendidos por cada proveedor
app.get('/total-medicamentos-vendidos-por-proveedor', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const comprasCollection = db.collection('Compras');
    const ventasCollection = db.collection('Ventas');

    const proveedores = await comprasCollection.distinct("proveedor.nombre", {});
    const totalMedicamentosVendidosPorProveedor = [];

    for (const proveedor of proveedores) {
      const medicamentosComprados = await comprasCollection.aggregate([
        { $match: { "proveedor.nombre": proveedor } },
        { $group: { _id: null, total: { $sum: "$medicamentosComprados.cantidadComprada" } } }
      ]).toArray();

      const medicamentosVendidos = await ventasCollection.aggregate([
        { $match: { "proveedor.nombre": proveedor } },
        { $group: { _id: null, total: { $sum: "$medicamentosVendidos.cantidadVendida" } } }
      ]).toArray();

      totalMedicamentosVendidosPorProveedor.push({
        proveedor,
        medicamentosComprados: medicamentosComprados[0]?.total || 0,
        medicamentosVendidos: medicamentosVendidos[0]?.total || 0
      });
    }

    res.json(totalMedicamentosVendidosPorProveedor);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

// Ruta para obtener la cantidad total de dinero recaudado por las ventas de medicamentos
app.get('/total-dinero-recaudado-ventas', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');
    const medicamentosCollection = db.collection('Medicamentos');

    const totalDineroRecaudadoVentas = await ventasCollection.aggregate([
      {
        $lookup: {
          from: "Medicamentos",
          localField: "medicamentosVendidos.nombreMedicamento",
          foreignField: "nombre",
          as: "medicamento"
        }
      },
      {
        $unwind: "$medicamento"
      },
      {
        $project: {
          _id: 0,
          total: {
            $multiply: ["$medicamento.precioVenta", "$medicamentosVendidos.cantidadVendida"]
          }
        }
      },
      {
        $group: {
          _id: null,
          total: { $sum: "$total" }
        }
      }
    ]).toArray();

    res.json(totalDineroRecaudadoVentas);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});
// Ruta para obtener medicamentos que no han sido vendidos
app.get('/medicamentos-no-vendidos', async (req, res) => {
  let client;

  try {
    client = new MongoClient(mongoURL);
    await client.connect();

    const db = client.db(dbName);
    const medicamentosCollection = db.collection('Medicamentos');
    const ventasCollection = db.collection('Ventas');

    // Obtén todos los nombres de medicamentos vendidos
    const medicamentosVendidos = await ventasCollection.distinct('medicamentosVendidos.nombreMedicamento', {});

    // Encuentra los medicamentos que no están en la lista de medicamentos vendidos
    const medicamentosNoVendidos = await medicamentosCollection.find({ "nombre": { $nin: medicamentosVendidos } }).toArray();

    res.json(medicamentosNoVendidos);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    if (client) {
      client.close();
    }
  }
});

// Ruta para obtener el medicamento más caro
app.get('/medicamento-mas-caro', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const medicamentosCollection = db.collection('Medicamentos');

    const medicamentoMasCaro = await medicamentosCollection.find({}).sort({ precioVenta: -1 }).limit(1).toArray();

    res.json(medicamentoMasCaro);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});
app.get('/medicamentos-por-proveedor', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const medicamentosCollection = db.collection('Medicamentos');

    const pipeline = [
      {
        $group: {
          _id: '$proveedor',
          count: { $sum: 1 }
        }
      }
    ];

    const medicamentosPorProveedor = await medicamentosCollection.aggregate(pipeline).toArray();

    res.json(medicamentosPorProveedor);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

app.get('/pacientes-que-compraron-paracetamol', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');

    const pacientesQueCompraronParacetamol = await ventasCollection.distinct('paciente', {
      'medicamentosVendidos.nombreMedicamento': 'Paracetamol'
    });

    res.json(pacientesQueCompraronParacetamol);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

app.get('/proveedores-sin-ventas-ultimo-ano', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const comprasCollection = db.collection('Compras');

    const proveedoresSinVentasUltimoAno = await comprasCollection.distinct('proveedor.nombre', {
      fechaCompra: { $gte: new Date(Date.now() - 365 * 24 * 60 * 60 * 1000) }
    });

    res.json(proveedoresSinVentasUltimoAno);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

app.get('/total-medicamentos-vendidos-marzo-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');

    const totalMedicamentosVendidosMarzo2023 = await ventasCollection.aggregate([
      {
        $match: {
          fechaVenta: {
            $gte: new Date('2023-03-01'),
            $lt: new Date('2023-04-01')
          }
        }
      },
      {
        $group: {
          _id: null,
          total: { $sum: '$medicamentosVendidos.cantidadVendida' }
        }
      }
    ]).toArray();

    res.json(totalMedicamentosVendidosMarzo2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

app.get('/medicamento-menos-vendido-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');

    const medicamentosVendidos2023 = await ventasCollection.aggregate([
      {
        $unwind: '$medicamentosVendidos'
      },
      {
        $match: {
          fechaVenta: {
            $gte: new Date('2023-01-01'),
            $lt: new Date('2024-01-01')
          }
        }
      },
      {
        $group: {
          _id: '$medicamentosVendidos.nombreMedicamento',
          total: { $sum: '$medicamentosVendidos.cantidadVendida' }
        }
      },
      {
        $sort: { total: 1 }
      },
      {
        $limit: 1
      }
    ]).toArray();

    res.json(medicamentosVendidos2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

app.get('/ganancia-total-por-proveedor-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const comprasCollection = db.collection('Compras');
    const proveedoresCollection = db.collection('Proveedores');

    const gananciasPorProveedor2023 = await comprasCollection.aggregate([
      {
        $match: {
          fechaCompra: {
            $gte: new Date('2023-01-01'),
            $lt: new Date('2024-01-01')
          }
        }
      },
      {
        $lookup: {
          from: 'Proveedores',
          localField: 'proveedor.nombre',
          foreignField: 'nombre',
          as: 'proveedor'
        }
      },
      {
        $unwind: '$proveedor'
      },
      {
        $project: {
          _id: 0,
          proveedor: '$proveedor.nombre',
          ganancia: { $multiply: ['$precioCompra', '$cantidadComprada'] }
        }
      },
      {
        $group: {
          _id: '$proveedor',
          totalGanancia: { $sum: '$ganancia' }
        }
      }
    ]).toArray();

    res.json(gananciasPorProveedor2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

app.get('/promedio-medicamentos-por-venta', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');

    const promedioMedicamentosPorVenta = await ventasCollection.aggregate([
      {
        $unwind: '$medicamentosVendidos'
      },
      {
        $group: {
          _id: null,
          promedio: {
            $avg: '$medicamentosVendidos.cantidadVendida'
          }
        }
      }
    ]).toArray();

    res.json(promedioMedicamentosPorVenta);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

app.get('/ventas-por-empleado-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');

    const ventasPorEmpleado2023 = await ventasCollection.aggregate([
      {
        $match: {
          fechaVenta: {
            $gte: new Date('2023-01-01'),
            $lt: new Date('2024-01-01')
          }
        }
      },
      {
        $group: {
          _id: '$empleado',
          totalVentas: { $sum: 1 }
        }
      }
    ]).toArray();

    res.json(ventasPorEmpleado2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});
app.get('/medicamentos-que-expiran-en-2024', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const medicamentosCollection = db.collection('Medicamentos');

    const medicamentosQueExpiranEn2024 = await medicamentosCollection.find({
      fechaCaducidad: {
        $gte: new Date('2024-01-01'),
        $lt: new Date('2025-01-01')
      }
    }).toArray();

    res.json(medicamentosQueExpiranEn2024);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});



app.get('/empleados-con-mas-de-5-ventas', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');

    const empleadosConMasDe5Ventas = await ventasCollection.aggregate([
      {
        $group: {
          _id: '$empleado',
          totalVentas: { $sum: 1 }
        }
      },
      {
        $match: {
          totalVentas: { $gt: 5 }
        }
      }
    ]).toArray();

    res.json(empleadosConMasDe5Ventas);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

app.get('/medicamentos-no-vendidos-nunca', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const medicamentosCollection = db.collection('Medicamentos');

    const medicamentosNoVendidosNunca = await medicamentosCollection.find({ "ventas": 0 }).toArray();

    res.json(medicamentosNoVendidosNunca);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

app.get('/paciente-que-gasto-mas-en-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');

    const pacienteQueGastoMasEn2023 = await ventasCollection.aggregate([
      {
        $match: {
          fechaVenta: {
            $gte: new Date('2023-01-01'),
            $lt: new Date('2024-01-01')
          }
        }
      },
      {
        $group: {
          _id: '$paciente',
          totalGasto: { $sum: '$montoTotal' }
        }
      },
      {
        $sort: { totalGasto: -1 }
      },
      {
        $limit: 1
      }
    ]).toArray();

    res.json(pacienteQueGastoMasEn2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

app.get('/empleados-sin-ventas-en-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const empleadosCollection = db.collection('Empleados');
    const ventasCollection = db.collection('Ventas');

    const empleadosSinVentasEn2023 = await empleadosCollection.aggregate([
      {
        $lookup: {
          from: 'Ventas',
          localField: 'nombre',
          foreignField: 'empleado',
          as: 'ventas'
        }
      },
      {
        $match: {
          'ventas.fechaVenta': {
            $lt: new Date('2023-01-01') || { $gt: new Date('2023-12-31') }
          }
        }
      },
      {
        $project: {
          _id: 0,
          nombre: 1
        }
      }
    ]).toArray();

    res.json(empleadosSinVentasEn2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

app.get('/pacientes-compraron-paracetamol-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');

    const pacientesParacetamol2023 = await ventasCollection.distinct("paciente", { "fechaVenta": { $gte: new Date("2023-01-01"), $lt: new Date("2024-01-01") }, "medicamentosVendidos.nombreMedicamento": "Paracetamol" });

    res.json(pacientesParacetamol2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

app.get('/total-medicamentos-vendidos-por-mes-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');

    const totalMedicamentosPorMes2023 = await ventasCollection.aggregate([
      {
        $match: {
          "fechaVenta": { $gte: new Date("2023-01-01"), $lt: new Date("2024-01-01") }
        }
      },
      {
        $group: {
          _id: { $month: "$fechaVenta" },
          total: { $sum: { $size: "$medicamentosVendidos" } }
        }
      }
    ]).toArray();

    res.json(totalMedicamentosPorMes2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});
app.get('/empleados-menos-5-ventas-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');

    const empleadosMenos5Ventas2023 = await ventasCollection.aggregate([
      {
        $match: {
          "fechaVenta": { $gte: new Date("2023-01-01"), $lt: new Date("2024-01-01") }
        }
      },
      {
        $group: {
          _id: "$empleado",
          totalVentas: { $sum: 1 }
        }
      },
      {
        $match: {
          totalVentas: { $lt: 5 }
        }
      }
    ]).toArray();

    res.json(empleadosMenos5Ventas2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

app.get('/numero-total-proveedores-medicamentos-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const comprasCollection = db.collection('Compras');

    const totalProveedoresMedicamentos2023 = await comprasCollection.distinct("proveedor.nombre", { "fechaCompra": { $gte: new Date("2023-01-01"), $lt: new Date("2024-01-01") } });

    res.json({ total: totalProveedoresMedicamentos2023.length });
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

app.get('/proveedores-medicamentos-bajos-stock', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const medicamentosCollection = db.collection('Medicamentos');

    const proveedoresMedicamentosBajosStock = await medicamentosCollection.distinct("proveedor", { "stock": { $lt: 50 } });

    res.json(proveedoresMedicamentosBajosStock);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

app.get('/pacientes-no-compraron-medicamentos-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');

    const pacientesNoCompraronMedicamentos2023 = await db.collection('Pacientes').find({}).toArray();

    // Filtrar pacientes que no compraron medicamentos en 2023
    for (const paciente of pacientesNoCompraronMedicamentos2023) {
      const ventasPaciente = await ventasCollection.findOne({ "paciente": paciente._id, "fechaVenta": { $gte: new Date("2023-01-01"), $lt: new Date("2024-01-01") } });
      if (ventasPaciente) {
        pacientesNoCompraronMedicamentos2023 = pacientesNoCompraronMedicamentos2023.filter(p => p._id !== paciente._id);
      }
    }

    res.json(pacientesNoCompraronMedicamentos2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

app.get('/medicamentos-vendidos-cada-mes-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');

    const meses = [
      new Date("2023-01-01"), new Date("2023-02-01"), new Date("2023-03-01"),
      new Date("2023-04-01"), new Date("2023-05-01"), new Date("2023-06-01"),
      new Date("2023-07-01"), new Date("2023-08-01"), new Date("2023-09-01"),
      new Date("2023-10-01"), new Date("2023-11-01"), new Date("2023-12-01")
    ];

    const medicamentosVendidosCadaMes2023 = [];

    for (const mes of meses) {
      const medicamentosMes = await ventasCollection.distinct("medicamentosVendidos.nombreMedicamento", {
        "fechaVenta": {
          $gte: mes,
          $lt: new Date(mes.getFullYear(), mes.getMonth() + 1, 1)
        }
      });

      medicamentosVendidosCadaMes2023.push({
        mes: mes.toLocaleString('es-ES', { month: 'long', year: 'numeric' }),
        medicamentos: medicamentosMes
      });
    }

    res.json(medicamentosVendidosCadaMes2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});


app.get('/empleado-mayor-cantidad-medicamentos-distintos-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');

    const empleadoConMasMedicamentos2023 = await ventasCollection.aggregate([
      {
        $match: {
          "fechaVenta": { $gte: new Date("2023-01-01"), $lt: new Date("2024-01-01") }
        }
      },
      {
        $group: {
          _id: "$empleado",
          medicamentosVendidos: { $addToSet: "$medicamentosVendidos.nombreMedicamento" }
        }
      },
      {
        $project: {
          _id: 1,
          count: { $size: "$medicamentosVendidos" }
        }
      },
      {
        $sort: { count: -1 }
      },
      {
        $limit: 1
      }
    ]).toArray();

    res.json(empleadoConMasMedicamentos2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

// 33. Total gastado por cada paciente en 2023.
app.get('/total-gastado-por-paciente-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');

    const totalGastadoPorPaciente2023 = await ventasCollection.aggregate([
      {
        $match: {
          "fechaVenta": {
            $gte: new Date("2023-01-01"),
            $lt: new Date("2024-01-01")
          }
        }
      },
      {
        $group: {
          _id: "$paciente",
          total: { $sum: { $multiply: ["$medicamentosVendidos.precioVenta", "$medicamentosVendidos.cantidadVendida"] } }
        }
      }
    ]).toArray();

    res.json(totalGastadoPorPaciente2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

// 34. Medicamentos que no han sido vendidos en 2023.
app.get('/medicamentos-no-vendidos-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const medicamentosCollection = db.collection('Medicamentos');
    const ventasCollection = db.collection('Ventas');

    const medicamentosVendidos2023 = await ventasCollection.distinct("medicamentosVendidos.nombreMedicamento", {
      "fechaVenta": {
        $gte: new Date("2023-01-01"),
        $lt: new Date("2024-01-01")
      }
    });

    const todosLosMedicamentos = await medicamentosCollection.distinct("nombre");

    const medicamentosNoVendidos2023 = todosLosMedicamentos.filter(medicamento => !medicamentosVendidos2023.includes(medicamento));

    res.json(medicamentosNoVendidos2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

// 35. Proveedores que han suministrado al menos 5 medicamentos diferentes en 2023.
app.get('/proveedores-que-suministraron-5-medicamentos-diferentes-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const comprasCollection = db.collection('Compras');

    const proveedoresMedicamentosDiferentes2023 = await comprasCollection.aggregate([
      {
        $match: {
          "fechaCompra": {
            $gte: new Date("2023-01-01"),
            $lt: new Date("2024-01-01")
          }
        }
      },
      {
        $group: {
          _id: "$proveedor.nombre",
          medicamentosDiferentes: { $addToSet: "$medicamentosComprados.nombreMedicamento" }
        }
      },
      {
        $match: {
          "medicamentosDiferentes": { $size: { $gte: 5 } }
        }
      }
    ]).toArray();

    res.json(proveedoresMedicamentosDiferentes2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

// 36. Total de medicamentos vendidos en el primer trimestre de 2023.
app.get('/total-medicamentos-vendidos-primer-trimestre-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const ventasCollection = db.collection('Ventas');

    const totalMedicamentosPrimerTrimestre2023 = await ventasCollection.aggregate([
      {
        $match: {
          "fechaVenta": {
            $gte: new Date("2023-01-01"),
            $lt: new Date("2023-04-01")
          }
        }
      },
      {
        $group: {
          _id: null,
          total: { $sum: { $size: "$medicamentosVendidos" } }
        }
      }
    ]).toArray();

    res.json(totalMedicamentosPrimerTrimestre2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

// 37. Empleados que no realizaron ventas en abril de 2023.
app.get('/empleados-sin-ventas-abril-2023', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const empleadosCollection = db.collection('Empleados');
    const ventasCollection = db.collection('Ventas');

    const empleadosSinVentasAbril2023 = await empleadosCollection.aggregate([
      {
        $lookup: {
          from: "Ventas",
          localField: "nombre",
          foreignField: "empleado",
          as: "ventas"
        }
      },
      {
        $match: {
          "ventas.fechaVenta": {
            $not: { $gte: new Date("2023-04-01"), $lt: new Date("2023-05-01") }
          }
        }
      },
      {
        $project: {
          _id: 0,
          nombre: 1
        }
      }
    ]).toArray();

    res.json(empleadosSinVentasAbril2023);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

// 38. Medicamentos con un precio mayor a 50 y un stock menor a 100.
app.get('/medicamentos-precio-mayor-50-stock-menor-100', async (req, res) => {
  try {
    conectarDB();
    const db = client.db(dbName);
    const medicamentosCollection = db.collection('Medicamentos');

    const medicamentosPrecioStock = await medicamentosCollection.find({
      "precioVenta": { $gt: 50 },
      "stock": { $lt: 100 }
    }).toArray();

    res.json(medicamentosPrecioStock);
  } catch (err) {
    console.error('Error en MongoDB:', err);
    res.status(500).send('Error en MongoDB');
  } finally {
    cerrarDB();
  }
});

// Iniciar el servidor Express
app.listen(port, () => {
  console.log(`Servidor Express escuchando en el puerto ${port}`);
});


